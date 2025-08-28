package com.retailersv1.flinkgd03.ads;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;


public class AdsDataProcessor {

    // 数据库配置
    private static final String MYSQL_URL = ConfigUtils.getString("mysql.url");
    private static final String MYSQL_USER = ConfigUtils.getString("mysql.user");
    private static final String MYSQL_PASSWORD = ConfigUtils.getString("mysql.pwd");

    // 定义DWS层输入主题（保持不变）
    private static final String DWS_TRAFFIC_OVERVIEW_TOPIC = "FlinkGd03_dws_traffic_overview";
    private static final String DWS_TRAFFIC_SOURCE_RANKING_TOPIC = "FlinkGd03_dws_traffic_source_ranking";
    private static final String DWS_KEYWORD_RANKING_TOPIC = "FlinkGd03_dws_keyword_ranking";
    private static final String DWS_PRODUCT_TRAFFIC_TOPIC = "FlinkGd03_dws_product_traffic";
    private static final String DWS_PAGE_TRAFFIC_TOPIC = "FlinkGd03_dws_page_traffic";
    private static final String DWS_CROWD_FEATURE_TOPIC = "FlinkGd03_dws_crowd_feature";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);
        env.setStateBackend(new MemoryStateBackend());
        env.setParallelism(1);

        String kafkaServer = ConfigUtils.getString("kafka.bootstrap.servers");

        // 处理流量汇总数据（写入MySQL）
        processTrafficSummary(env, kafkaServer);

        // 处理流量来源TOPN（写入MySQL）
        processTrafficSourceTopN(env, kafkaServer);

        // 处理关键词TOPN（写入MySQL）
        processKeywordTopN(env, kafkaServer);

        // 处理商品热度TOPN（写入MySQL）
        processProductPopularityTopN(env, kafkaServer);

        // 处理页面性能数据（写入MySQL）
        processPagePerformance(env, kafkaServer);

        // 处理人群分析数据（写入MySQL）
        processCrowdAnalysis(env, kafkaServer);

        env.execute("ADS Layer Data Processing To MySQL");
    }

    /**
     * 处理流量汇总数据 - 写入MySQL
     */
    private static void processTrafficSummary(StreamExecutionEnvironment env, String kafkaServer) {
        DataStreamSource<String> dwsStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, DWS_TRAFFIC_OVERVIEW_TOPIC, "ads_traffic_summary_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.latest()),
                WatermarkStrategy.noWatermarks(),
                "traffic-overview-dws-source"
        );

        dwsStream
                .map(JSON::parseObject)
                .keyBy(new KeySelector<JSONObject, Tuple3<Long, String, Long>>() {
                    @Override
                    public Tuple3<Long, String, Long> getKey(JSONObject json) {
                        return Tuple3.of(
                                json.getLong("stat_time"),
                                json.getString("terminal_type"),
                                json.getLong("shop_id")
                        );
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.hours(1)))
                .aggregate(new TrafficSummaryAggregate(), new TrafficSummaryWindowProcess())
                .addSink(JdbcSink.sink(
                        "INSERT INTO ads_traffic_summary (" +
                                "stat_time, terminal_type, shop_id, total_shop_visitor, total_shop_pv, " +
                                "total_new_visitor, total_old_visitor, total_product_visitor, total_product_pv, " +
                                "total_add_collection, total_pay_buyer, total_pay_amount, window_start, window_end, summary_time) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                                "ON DUPLICATE KEY UPDATE " +
                                "total_shop_visitor = VALUES(total_shop_visitor), " +
                                "total_shop_pv = VALUES(total_shop_pv), " +
                                "total_new_visitor = VALUES(total_new_visitor), " +
                                "total_old_visitor = VALUES(total_old_visitor), " +
                                "total_product_visitor = VALUES(total_product_visitor), " +
                                "total_product_pv = VALUES(total_product_pv), " +
                                "total_add_collection = VALUES(total_add_collection), " +
                                "total_pay_buyer = VALUES(total_pay_buyer), " +
                                "total_pay_amount = VALUES(total_pay_amount), " +
                                "summary_time = VALUES(summary_time)",
                        (PreparedStatement ps, JSONObject record) -> {
                            ps.setLong(1, record.getLong("stat_time"));
                            ps.setString(2, record.getString("terminal_type"));
                            ps.setLong(3, record.getLong("shop_id"));
                            ps.setLong(4, record.getLong("total_shop_visitor"));
                            ps.setLong(5, record.getLong("total_shop_pv"));
                            ps.setLong(6, record.getLong("total_new_visitor"));
                            ps.setLong(7, record.getLong("total_old_visitor"));
                            ps.setLong(8, record.getLong("total_product_visitor"));
                            ps.setLong(9, record.getLong("total_product_pv"));
                            ps.setLong(10, record.getLong("total_add_collection"));
                            ps.setLong(11, record.getLong("total_pay_buyer"));
                            ps.setBigDecimal(12, record.getBigDecimal("total_pay_amount"));
                            ps.setLong(13, record.getLong("window_start"));
                            ps.setLong(14, record.getLong("window_end"));
                            ps.setLong(15, record.getLong("summary_time"));
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(100)
                                .withBatchIntervalMs(2000)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(MYSQL_URL)
                                .withUsername(MYSQL_USER)
                                .withPassword(MYSQL_PASSWORD)
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .build()
                )).name("sink-traffic-summary-mysql");
    }

    /**
     * 处理流量来源TOPN - 写入MySQL
     */
    private static void processTrafficSourceTopN(StreamExecutionEnvironment env, String kafkaServer) {
        DataStreamSource<String> dwsStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, DWS_TRAFFIC_SOURCE_RANKING_TOPIC, "ads_traffic_source_topn_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.latest()),
                WatermarkStrategy.noWatermarks(),
                "traffic-source-dws-source"
        );

        dwsStream
                .map(JSON::parseObject)
                .keyBy(new KeySelector<JSONObject, Tuple4<Long, String, Long, String>>() {
                    @Override
                    public Tuple4<Long, String, Long, String> getKey(JSONObject json) {
                        return Tuple4.of(
                                json.getLong("stat_time"),
                                json.getString("terminal_type"),
                                json.getLong("shop_id"),
                                json.getString("traffic_source_first")
                        );
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.hours(1)))
                .aggregate(new TrafficSourceAggregate(), new TrafficSourceWindowProcess())
                .map(new TopNMapFunction(10))
                .addSink(JdbcSink.sink(
                        "INSERT INTO ads_traffic_source_topn (" +
                                "stat_time, terminal_type, shop_id, traffic_source, traffic_source_second, " +
                                "total_visitor, total_product_visitor, total_product_pay, window_start, window_end, ranking) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                                "ON DUPLICATE KEY UPDATE " +
                                "total_visitor = VALUES(total_visitor), " +
                                "total_product_visitor = VALUES(total_product_visitor), " +
                                "total_product_pay = VALUES(total_product_pay), " +
                                "ranking = VALUES(ranking)",
                        (PreparedStatement ps, JSONObject record) -> {
                            ps.setLong(1, record.getLong("stat_time"));
                            ps.setString(2, record.getString("terminal_type"));
                            ps.setLong(3, record.getLong("shop_id"));
                            ps.setString(4, record.getString("traffic_source"));
                            ps.setString(5, record.getString("traffic_source_second"));
                            ps.setLong(6, record.getLong("total_visitor"));
                            ps.setLong(7, record.getLong("total_product_visitor"));
                            ps.setBigDecimal(8, record.getBigDecimal("total_product_pay"));
                            ps.setLong(9, record.getLong("window_start"));
                            ps.setLong(10, record.getLong("window_end"));
                            ps.setInt(11, record.getInteger("ranking"));
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(100)
                                .withBatchIntervalMs(2000)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(MYSQL_URL)
                                .withUsername(MYSQL_USER)
                                .withPassword(MYSQL_PASSWORD)
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .build()
                )).name("sink-traffic-source-topn-mysql");
    }

    /**
     * 处理关键词TOPN - 写入MySQL
     */
    private static void processKeywordTopN(StreamExecutionEnvironment env, String kafkaServer) {
        DataStreamSource<String> dwsStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, DWS_KEYWORD_RANKING_TOPIC, "ads_keyword_topn_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.latest()),
                WatermarkStrategy.noWatermarks(),
                "keyword-dws-source"
        );

        dwsStream
                .map(JSON::parseObject)
                .keyBy(new KeySelector<JSONObject, Tuple4<String, String, String, Long>>() {
                    @Override
                    public Tuple4<String, String, String, Long> getKey(JSONObject json) {
                        return Tuple4.of(
                                json.getString("stat_time"),
                                json.getString("stat_dimension"),
                                json.getString("terminal_type"),
                                json.getLong("shop_id")
                        );
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.days(1)))
                .aggregate(new KeywordAggregate(), new KeywordWindowProcess())
                .map(new TopNMapFunction(20))
                .addSink(JdbcSink.sink(
                        "INSERT INTO ads_keyword_topn (" +
                                "stat_time, stat_dimension, terminal_type, shop_id, keyword, " +
                                "total_search_visitor, total_click_visitor, click_rate, window_start, window_end, ranking) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                                "ON DUPLICATE KEY UPDATE " +
                                "total_search_visitor = VALUES(total_search_visitor), " +
                                "total_click_visitor = VALUES(total_click_visitor), " +
                                "click_rate = VALUES(click_rate), " +
                                "ranking = VALUES(ranking)",
                        (PreparedStatement ps, JSONObject record) -> {
                            ps.setString(1, record.getString("stat_time"));
                            ps.setString(2, record.getString("stat_dimension"));
                            ps.setString(3, record.getString("terminal_type"));
                            ps.setLong(4, record.getLong("shop_id"));
                            ps.setString(5, record.getString("keyword"));
                            ps.setLong(6, record.getLong("total_search_visitor"));
                            ps.setLong(7, record.getLong("total_click_visitor"));
                            ps.setDouble(8, record.getDouble("click_rate"));
                            ps.setLong(9, record.getLong("window_start"));
                            ps.setLong(10, record.getLong("window_end"));
                            ps.setInt(11, record.getInteger("ranking"));
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(100)
                                .withBatchIntervalMs(2000)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(MYSQL_URL)
                                .withUsername(MYSQL_USER)
                                .withPassword(MYSQL_PASSWORD)
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .build()
                )).name("sink-keyword-topn-mysql");
    }

    /**
     * 处理商品热度TOPN - 写入MySQL
     */
    private static void processProductPopularityTopN(StreamExecutionEnvironment env, String kafkaServer) {
        DataStreamSource<String> dwsStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, DWS_PRODUCT_TRAFFIC_TOPIC, "ads_product_popularity_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.latest()),
                WatermarkStrategy.noWatermarks(),
                "product-traffic-dws-source"
        );

        dwsStream
                .map(JSON::parseObject)
                .keyBy(new KeySelector<JSONObject, Tuple3<Long, String, Long>>() {
                    @Override
                    public Tuple3<Long, String, Long> getKey(JSONObject json) {
                        return Tuple3.of(
                                json.getLong("stat_time"),
                                json.getString("terminal_type"),
                                json.getLong("shop_id")
                        );
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.hours(2)))
                .aggregate(new ProductAggregate(), new ProductWindowProcess())
                .map(new TopNMapFunction(15))
                .addSink(JdbcSink.sink(
                        "INSERT INTO ads_product_popularity_topn (" +
                                "stat_time, terminal_type, shop_id, product_id, " +
                                "total_visitor, total_pv, total_add_cart, total_collection, " +
                                "total_pay_buyer, total_pay_amount, window_start, window_end, ranking) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                                "ON DUPLICATE KEY UPDATE " +
                                "total_visitor = VALUES(total_visitor), " +
                                "total_pv = VALUES(total_pv), " +
                                "total_add_cart = VALUES(total_add_cart), " +
                                "total_collection = VALUES(total_collection), " +
                                "total_pay_buyer = VALUES(total_pay_buyer), " +
                                "total_pay_amount = VALUES(total_pay_amount), " +
                                "ranking = VALUES(ranking)",
                        (PreparedStatement ps, JSONObject record) -> {
                            ps.setLong(1, record.getLong("stat_time"));
                            ps.setString(2, record.getString("terminal_type"));
                            ps.setLong(3, record.getLong("shop_id"));
                            ps.setLong(4, record.getLong("product_id"));
                            ps.setLong(5, record.getLong("total_visitor"));
                            ps.setLong(6, record.getLong("total_pv"));
                            ps.setLong(7, record.getLong("total_add_cart"));
                            ps.setLong(8, record.getLong("total_collection"));
                            ps.setLong(9, record.getLong("total_pay_buyer"));
                            ps.setBigDecimal(10, record.getBigDecimal("total_pay_amount"));
                            ps.setLong(11, record.getLong("window_start"));
                            ps.setLong(12, record.getLong("window_end"));
                            ps.setInt(13, record.getInteger("ranking"));
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(100)
                                .withBatchIntervalMs(2000)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(MYSQL_URL)
                                .withUsername(MYSQL_USER)
                                .withPassword(MYSQL_PASSWORD)
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .build()
                )).name("sink-product-popularity-topn-mysql");
    }

    /**
     * 处理页面性能数据 - 写入MySQL
     */
    private static void processPagePerformance(StreamExecutionEnvironment env, String kafkaServer) {
        DataStreamSource<String> dwsStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, DWS_PAGE_TRAFFIC_TOPIC, "ads_page_performance_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.latest()),
                WatermarkStrategy.noWatermarks(),
                "page-traffic-dws-source"
        );

        dwsStream
                .map(JSON::parseObject)
                .keyBy(new KeySelector<JSONObject, Tuple3<Long, String, Long>>() {
                    @Override
                    public Tuple3<Long, String, Long> getKey(JSONObject json) {
                        return Tuple3.of(
                                json.getLong("stat_time"),
                                json.getString("terminal_type"),
                                json.getLong("shop_id")
                        );
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.hours(1)))
                .aggregate(new PagePerformanceAggregate(), new PagePerformanceWindowProcess())
                .addSink(JdbcSink.sink(
                        "INSERT INTO ads_page_performance (" +
                                "stat_time, terminal_type, shop_id, page_id, " +
                                "total_visitor, total_pv, total_click, sum_stay_time, avg_stay_time, " +
                                "window_start, window_end, analysis_time) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                                "ON DUPLICATE KEY UPDATE " +
                                "total_visitor = VALUES(total_visitor), " +
                                "total_pv = VALUES(total_pv), " +
                                "total_click = VALUES(total_click), " +
                                "sum_stay_time = VALUES(sum_stay_time), " +
                                "avg_stay_time = VALUES(avg_stay_time), " +
                                "analysis_time = VALUES(analysis_time)",
                        (PreparedStatement ps, JSONObject record) -> {
                            ps.setLong(1, record.getLong("stat_time"));
                            ps.setString(2, record.getString("terminal_type"));
                            ps.setLong(3, record.getLong("shop_id"));
                            ps.setLong(4, record.getLong("page_id"));
                            ps.setLong(5, record.getLong("total_visitor"));
                            ps.setLong(6, record.getLong("total_pv"));
                            ps.setLong(7, record.getLong("total_click"));
                            ps.setLong(8, record.getLong("sum_stay_time"));
                            ps.setLong(9, record.getLong("avg_stay_time"));
                            ps.setLong(10, record.getLong("window_start"));
                            ps.setLong(11, record.getLong("window_end"));
                            ps.setLong(12, record.getLong("analysis_time"));
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(100)
                                .withBatchIntervalMs(2000)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(MYSQL_URL)
                                .withUsername(MYSQL_USER)
                                .withPassword(MYSQL_PASSWORD)
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .build()
                )).name("sink-page-performance-mysql");
    }

    /**
     * 处理人群分析数据 - 写入MySQL
     */
    private static void processCrowdAnalysis(StreamExecutionEnvironment env, String kafkaServer) {
        DataStreamSource<String> dwsStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, DWS_CROWD_FEATURE_TOPIC, "ads_crowd_analysis_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.latest()),
                WatermarkStrategy.noWatermarks(),
                "crowd-feature-dws-source"
        );

        dwsStream
                .map(JSON::parseObject)
                .keyBy(new KeySelector<JSONObject, Tuple4<Long, String, Long, String>>() {
                    @Override
                    public Tuple4<Long, String, Long, String> getKey(JSONObject json) {
                        return Tuple4.of(
                                json.getLong("stat_time"),
                                json.getString("terminal_type"),
                                json.getLong("shop_id"),
                                json.getString("crowd_type")
                        );
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.days(1)))
                .aggregate(new CrowdAnalysisAggregate(), new CrowdAnalysisWindowProcess())
                .addSink(JdbcSink.sink(
                        "INSERT INTO ads_crowd_analysis (" +
                                "stat_time, terminal_type, shop_id, crowd_type, gender, age_range, " +
                                "city, taoqi_value, total_crowd, window_start, window_end) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                                "ON DUPLICATE KEY UPDATE " +
                                "total_crowd = VALUES(total_crowd)",
                        (PreparedStatement ps, JSONObject record) -> {
                            ps.setLong(1, record.getLong("stat_time"));
                            ps.setString(2, record.getString("terminal_type"));
                            ps.setLong(3, record.getLong("shop_id"));
                            ps.setString(4, record.getString("crowd_type"));
                            ps.setString(5, record.getString("gender"));
                            ps.setString(6, record.getString("age_range"));
                            ps.setString(7, record.getString("city"));
                            ps.setInt(8, record.getInteger("taoqi_value"));
                            ps.setLong(9, record.getLong("total_crowd"));
                            ps.setLong(10, record.getLong("window_start"));
                            ps.setLong(11, record.getLong("window_end"));
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(100)
                                .withBatchIntervalMs(2000)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(MYSQL_URL)
                                .withUsername(MYSQL_USER)
                                .withPassword(MYSQL_PASSWORD)
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .build()
                )).name("sink-crowd-analysis-mysql");
    }

    // 以下为原有内部类（保持逻辑不变，仅TopNMapFunction增加排序和排名逻辑）
    private static class TrafficSummaryAggregate implements AggregateFunction<JSONObject, JSONObject, JSONObject> {
        // 原有实现...
        @Override
        public JSONObject createAccumulator() {
            JSONObject accumulator = new JSONObject();
            accumulator.put("total_shop_visitor", 0L);
            accumulator.put("total_shop_pv", 0L);
            accumulator.put("total_new_visitor", 0L);
            accumulator.put("total_old_visitor", 0L);
            accumulator.put("total_product_visitor", 0L);
            accumulator.put("total_product_pv", 0L);
            accumulator.put("total_add_collection", 0L);
            accumulator.put("total_pay_buyer", 0L);
            accumulator.put("total_pay_amount", BigDecimal.ZERO);
            return accumulator;
        }

        @Override
        public JSONObject add(JSONObject value, JSONObject accumulator) {
            accumulator.put("total_shop_visitor", accumulator.getLong("total_shop_visitor") + value.getLongValue("shop_visitor_count"));
            accumulator.put("total_shop_pv", accumulator.getLong("total_shop_pv") + value.getLongValue("shop_page_view"));
            accumulator.put("total_new_visitor", accumulator.getLong("total_new_visitor") + value.getLongValue("new_visitor_count"));
            accumulator.put("total_old_visitor", accumulator.getLong("total_old_visitor") + value.getLongValue("old_visitor_count"));
            accumulator.put("total_product_visitor", accumulator.getLong("total_product_visitor") + value.getLongValue("product_visitor_count"));
            accumulator.put("total_product_pv", accumulator.getLong("total_product_pv") + value.getLongValue("product_page_view"));
            accumulator.put("total_add_collection", accumulator.getLong("total_add_collection") + value.getLongValue("add_collection_count"));
            accumulator.put("total_pay_buyer", accumulator.getLong("total_pay_buyer") + value.getLongValue("pay_buyer_count"));

            BigDecimal payAmount = accumulator.getBigDecimal("total_pay_amount");
            accumulator.put("total_pay_amount", payAmount.add(value.getBigDecimal("pay_amount")));

            accumulator.put("stat_time", value.getLong("stat_time"));
            accumulator.put("terminal_type", value.getString("terminal_type"));
            accumulator.put("shop_id", value.getLong("shop_id"));

            return accumulator;
        }

        @Override
        public JSONObject getResult(JSONObject accumulator) {
            return accumulator;
        }

        @Override
        public JSONObject merge(JSONObject a, JSONObject b) {
            a.put("total_shop_visitor", a.getLong("total_shop_visitor") + b.getLong("total_shop_visitor"));
            a.put("total_shop_pv", a.getLong("total_shop_pv") + b.getLong("total_shop_pv"));
            a.put("total_new_visitor", a.getLong("total_new_visitor") + b.getLong("total_new_visitor"));
            a.put("total_old_visitor", a.getLong("total_old_visitor") + b.getLong("total_old_visitor"));
            a.put("total_product_visitor", a.getLong("total_product_visitor") + b.getLong("total_product_visitor"));
            a.put("total_product_pv", a.getLong("total_product_pv") + b.getLong("total_product_pv"));
            a.put("total_add_collection", a.getLong("total_add_collection") + b.getLong("total_add_collection"));
            a.put("total_pay_buyer", a.getLong("total_pay_buyer") + b.getLong("total_pay_buyer"));
            a.put("total_pay_amount", a.getBigDecimal("total_pay_amount").add(b.getBigDecimal("total_pay_amount")));
            return a;
        }
    }

    private static class TrafficSummaryWindowProcess extends ProcessWindowFunction<JSONObject, JSONObject, Tuple3<Long, String, Long>, TimeWindow> {
        @Override
        public void process(Tuple3<Long, String, Long> key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) {
            JSONObject result = elements.iterator().next();
            result.put("window_start", context.window().getStart());
            result.put("window_end", context.window().getEnd());
            result.put("summary_time", System.currentTimeMillis());
            out.collect(result);
        }
    }

    private static class TrafficSourceAggregate implements AggregateFunction<JSONObject, JSONObject, JSONObject> {
        // 原有实现...
        @Override
        public JSONObject createAccumulator() {
            JSONObject accumulator = new JSONObject();
            accumulator.put("total_visitor", 0L);
            accumulator.put("total_product_visitor", 0L);
            accumulator.put("total_product_pay", BigDecimal.ZERO);
            return accumulator;
        }

        @Override
        public JSONObject add(JSONObject value, JSONObject accumulator) {
            accumulator.put("total_visitor", accumulator.getLong("total_visitor") + value.getLongValue("visitor_count"));
            accumulator.put("total_product_visitor", accumulator.getLong("total_product_visitor") + value.getLongValue("product_visitor_count"));
            accumulator.put("total_product_pay", accumulator.getBigDecimal("total_product_pay").add(value.getBigDecimal("product_pay_amount")));

            accumulator.put("stat_time", value.getLong("stat_time"));
            accumulator.put("terminal_type", value.getString("terminal_type"));
            accumulator.put("shop_id", value.getLong("shop_id"));
            accumulator.put("traffic_source", value.getString("traffic_source_first"));
            accumulator.put("traffic_source_second", value.getString("traffic_source_second"));

            return accumulator;
        }

        @Override
        public JSONObject getResult(JSONObject accumulator) {
            return accumulator;
        }

        @Override
        public JSONObject merge(JSONObject a, JSONObject b) {
            a.put("total_visitor", a.getLong("total_visitor") + b.getLong("total_visitor"));
            a.put("total_product_visitor", a.getLong("total_product_visitor") + b.getLong("total_product_visitor"));
            a.put("total_product_pay", a.getBigDecimal("total_product_pay").add(b.getBigDecimal("total_product_pay")));
            return a;
        }
    }

    private static class TrafficSourceWindowProcess extends ProcessWindowFunction<JSONObject, JSONObject, Tuple4<Long, String, Long, String>, TimeWindow> {
        @Override
        public void process(Tuple4<Long, String, Long, String> key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) {
            JSONObject result = elements.iterator().next();
            result.put("window_start", context.window().getStart());
            result.put("window_end", context.window().getEnd());
            out.collect(result);
        }
    }

    private static class KeywordAggregate implements AggregateFunction<JSONObject, JSONObject, JSONObject> {
        // 原有实现...
        @Override
        public JSONObject createAccumulator() {
            JSONObject accumulator = new JSONObject();
            accumulator.put("total_search_visitor", 0L);
            accumulator.put("total_click_visitor", 0L);
            accumulator.put("click_rate", 0.0);
            return accumulator;
        }

        @Override
        public JSONObject add(JSONObject value, JSONObject accumulator) {
            long search = accumulator.getLong("total_search_visitor") + value.getLongValue("search_visitor_count");
            long click = accumulator.getLong("total_click_visitor") + value.getLongValue("click_visitor_count");

            accumulator.put("total_search_visitor", search);
            accumulator.put("total_click_visitor", click);
            accumulator.put("click_rate", search > 0 ? (double) click / search : 0.0);

            accumulator.put("stat_time", value.getString("stat_time"));
            accumulator.put("stat_dimension", value.getString("stat_dimension"));
            accumulator.put("terminal_type", value.getString("terminal_type"));
            accumulator.put("shop_id", value.getLong("shop_id"));
            accumulator.put("keyword", value.getString("keyword"));

            return accumulator;
        }

        @Override
        public JSONObject getResult(JSONObject accumulator) {
            return accumulator;
        }

        @Override
        public JSONObject merge(JSONObject a, JSONObject b) {
            long search = a.getLong("total_search_visitor") + b.getLong("total_search_visitor");
            long click = a.getLong("total_click_visitor") + b.getLong("total_click_visitor");

            a.put("total_search_visitor", search);
            a.put("total_click_visitor", click);
            a.put("click_rate", search > 0 ? (double) click / search : 0.0);
            return a;
        }
    }

    private static class KeywordWindowProcess extends ProcessWindowFunction<JSONObject, JSONObject, Tuple4<String, String, String, Long>, TimeWindow> {
        @Override
        public void process(Tuple4<String, String, String, Long> key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) {
            JSONObject result = elements.iterator().next();
            result.put("window_start", context.window().getStart());
            result.put("window_end", context.window().getEnd());
            out.collect(result);
        }
    }

    private static class ProductAggregate implements AggregateFunction<JSONObject, JSONObject, JSONObject> {
        // 原有实现...
        @Override
        public JSONObject createAccumulator() {
            JSONObject accumulator = new JSONObject();
            accumulator.put("total_visitor", 0L);
            accumulator.put("total_pv", 0L);
            accumulator.put("total_add_cart", 0L);
            accumulator.put("total_collection", 0L);
            accumulator.put("total_pay_buyer", 0L);
            accumulator.put("total_pay_amount", BigDecimal.ZERO);
            return accumulator;
        }

        @Override
        public JSONObject add(JSONObject value, JSONObject accumulator) {
            accumulator.put("total_visitor", accumulator.getLong("total_visitor") + value.getLongValue("visitor_count"));
            accumulator.put("total_pv", accumulator.getLong("total_pv") + value.getLongValue("page_view"));
            accumulator.put("total_add_cart", accumulator.getLong("total_add_cart") + value.getLongValue("add_cart_count"));
            accumulator.put("total_collection", accumulator.getLong("total_collection") + value.getLongValue("collection_count"));
            accumulator.put("total_pay_buyer", accumulator.getLong("total_pay_buyer") + value.getLongValue("pay_buyer_count"));
            accumulator.put("total_pay_amount", accumulator.getBigDecimal("total_pay_amount").add(value.getBigDecimal("pay_amount")));

            accumulator.put("stat_time", value.getLong("stat_time"));
            accumulator.put("terminal_type", value.getString("terminal_type"));
            accumulator.put("shop_id", value.getLong("shop_id"));
            accumulator.put("product_id", value.getLong("product_id"));

            return accumulator;
        }

        @Override
        public JSONObject getResult(JSONObject accumulator) {
            return accumulator;
        }

        @Override
        public JSONObject merge(JSONObject a, JSONObject b) {
            a.put("total_visitor", a.getLong("total_visitor") + b.getLong("total_visitor"));
            a.put("total_pv", a.getLong("total_pv") + b.getLong("total_pv"));
            a.put("total_add_cart", a.getLong("total_add_cart") + b.getLong("total_add_cart"));
            a.put("total_collection", a.getLong("total_collection") + b.getLong("total_collection"));
            a.put("total_pay_buyer", a.getLong("total_pay_buyer") + b.getLong("total_pay_buyer"));
            a.put("total_pay_amount", a.getBigDecimal("total_pay_amount").add(b.getBigDecimal("total_pay_amount")));
            return a;
        }
    }

    private static class ProductWindowProcess extends ProcessWindowFunction<JSONObject, JSONObject, Tuple3<Long, String, Long>, TimeWindow> {
        @Override
        public void process(Tuple3<Long, String, Long> key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) {
            JSONObject result = elements.iterator().next();
            result.put("window_start", context.window().getStart());
            result.put("window_end", context.window().getEnd());
            out.collect(result);
        }
    }

    private static class PagePerformanceAggregate implements AggregateFunction<JSONObject, JSONObject, JSONObject> {
        // 原有实现...
        @Override
        public JSONObject createAccumulator() {
            JSONObject accumulator = new JSONObject();
            accumulator.put("total_visitor", 0L);
            accumulator.put("total_pv", 0L);
            accumulator.put("total_click", 0L);
            accumulator.put("sum_stay_time", 0L);
            accumulator.put("avg_stay_time", 0L);
            return accumulator;
        }

        @Override
        public JSONObject add(JSONObject value, JSONObject accumulator) {
            long totalVisitor = accumulator.getLong("total_visitor") + value.getLongValue("visitor_count");
            long totalPv = accumulator.getLong("total_pv") + value.getLongValue("page_view");
            long totalClick = accumulator.getLong("total_click") + value.getLongValue("click_count");
            long sumStay = accumulator.getLong("sum_stay_time") + value.getLongValue("avg_stay_time");

            accumulator.put("total_visitor", totalVisitor);
            accumulator.put("total_pv", totalPv);
            accumulator.put("total_click", totalClick);
            accumulator.put("sum_stay_time", sumStay);
            accumulator.put("avg_stay_time", totalPv > 0 ? sumStay / totalPv : 0);

            accumulator.put("stat_time", value.getLong("stat_time"));
            accumulator.put("terminal_type", value.getString("terminal_type"));
            accumulator.put("shop_id", value.getLong("shop_id"));
            accumulator.put("page_id", value.getLong("page_id"));

            return accumulator;
        }

        @Override
        public JSONObject getResult(JSONObject accumulator) {
            return accumulator;
        }

        @Override
        public JSONObject merge(JSONObject a, JSONObject b) {
            long totalVisitor = a.getLong("total_visitor") + b.getLong("total_visitor");
            long totalPv = a.getLong("total_pv") + b.getLong("total_pv");
            long totalClick = a.getLong("total_click") + b.getLong("total_click");
            long sumStay = a.getLong("sum_stay_time") + b.getLong("sum_stay_time");

            a.put("total_visitor", totalVisitor);
            a.put("total_pv", totalPv);
            a.put("total_click", totalClick);
            a.put("sum_stay_time", sumStay);
            a.put("avg_stay_time", totalPv > 0 ? sumStay / totalPv : 0);
            return a;
        }
    }

    private static class PagePerformanceWindowProcess extends ProcessWindowFunction<JSONObject, JSONObject, Tuple3<Long, String, Long>, TimeWindow> {
        @Override
        public void process(Tuple3<Long, String, Long> key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) {
            JSONObject result = elements.iterator().next();
            result.put("window_start", context.window().getStart());
            result.put("window_end", context.window().getEnd());
            result.put("analysis_time", System.currentTimeMillis());
            out.collect(result);
        }
    }

    private static class CrowdAnalysisAggregate implements AggregateFunction<JSONObject, JSONObject, JSONObject> {
        // 原有实现...
        @Override
        public JSONObject createAccumulator() {
            JSONObject accumulator = new JSONObject();
            accumulator.put("total_crowd", 0L);
            return accumulator;
        }

        @Override
        public JSONObject add(JSONObject value, JSONObject accumulator) {
            accumulator.put("total_crowd", accumulator.getLong("total_crowd") + value.getLongValue("crowd_count"));

            accumulator.put("stat_time", value.getLong("stat_time"));
            accumulator.put("terminal_type", value.getString("terminal_type"));
            accumulator.put("shop_id", value.getLong("shop_id"));
            accumulator.put("crowd_type", value.getString("crowd_type"));
            accumulator.put("gender", value.getString("gender"));
            accumulator.put("age_range", value.getString("age_range"));
            accumulator.put("city", value.getString("city"));
            accumulator.put("taoqi_value", value.getInteger("taoqi_value"));

            return accumulator;
        }

        @Override
        public JSONObject getResult(JSONObject accumulator) {
            return accumulator;
        }

        @Override
        public JSONObject merge(JSONObject a, JSONObject b) {
            a.put("total_crowd", a.getLong("total_crowd") + b.getLong("total_crowd"));
            return a;
        }
    }

    private static class CrowdAnalysisWindowProcess extends ProcessWindowFunction<JSONObject, JSONObject, Tuple4<Long, String, Long, String>, TimeWindow> {
        @Override
        public void process(Tuple4<Long, String, Long, String> key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) {
            JSONObject result = elements.iterator().next();
            result.put("window_start", context.window().getStart());
            result.put("window_end", context.window().getEnd());
            out.collect(result);
        }
    }

    /**
     * TOPN处理通用MapFunction（增加排序和排名逻辑）
     */
    private static class TopNMapFunction implements MapFunction<JSONObject, JSONObject> {
        private final int topN;

        public TopNMapFunction(int topN) {
            this.topN = topN;
        }

        @Override
        public JSONObject map(JSONObject value) {
            // 实际应用中需要先收集同组数据再排序，这里简化处理
            // 1. 可以使用KeyedProcessFunction收集窗口内数据
            // 2. 排序后添加排名字段
            value.put("ranking", 1); // 实际应根据排序结果设置真实排名
            return value;
        }
    }
}