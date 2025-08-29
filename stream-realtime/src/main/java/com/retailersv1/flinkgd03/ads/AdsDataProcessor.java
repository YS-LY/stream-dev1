package com.retailersv1.flinkgd03.ads;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


public class AdsDataProcessor {

    // ------------------------------ 1. 基础配置（数据库、Kafka主题、TopN参数） ------------------------------
    // 数据库配置（确保配置文件中URL格式：jdbc:mysql://ip:3306/db?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true）
    private static final String MYSQL_URL = ConfigUtils.getString("mysql.url");
    private static final String MYSQL_USER = ConfigUtils.getString("mysql.user");
    private static final String MYSQL_PASSWORD = ConfigUtils.getString("mysql.pwd");

    // DWS层Kafka主题（与上游DWS层输出一致）
    private static final String DWS_TRAFFIC_OVERVIEW_TOPIC = "FlinkGd03_dws_traffic_overview";
    private static final String DWS_TRAFFIC_SOURCE_RANKING_TOPIC = "FlinkGd03_dws_traffic_source_ranking";
    private static final String DWS_KEYWORD_RANKING_TOPIC = "FlinkGd03_dws_keyword_ranking";
    private static final String DWS_PRODUCT_TRAFFIC_TOPIC = "FlinkGd03_dws_product_traffic";
    private static final String DWS_PAGE_TRAFFIC_TOPIC = "FlinkGd03_dws_page_traffic";
    private static final String DWS_CROWD_FEATURE_TOPIC = "FlinkGd03_dws_crowd_feature";

    // TopN配置（测试用缩小TopN数量，加速验证）
    private static final int TRAFFIC_SOURCE_TOPN = 5;     // 流量来源TOP5（原10）
    private static final int KEYWORD_TOPN = 5;           // 关键词TOP5（原20）
    private static final int PRODUCT_POPULARITY_TOPN = 5;// 商品热度TOP5（原15）


    // ------------------------------ 2. 主函数（环境初始化+前置验证+任务提交） ------------------------------
    public static void main(String[] args) throws Exception {
        // 1. 前置验证：打印关键配置，确认读取正确（避免配置为空）
        System.out.println("=== 配置信息校验 ===");
        System.out.println("MySQL URL: " + MYSQL_URL);
        System.out.println("MySQL User: " + MYSQL_USER);
        String kafkaServer = ConfigUtils.getString("kafka.bootstrap.servers");
        System.out.println("Kafka Servers: " + kafkaServer);
        System.out.println("===================\n");

        // 2. 前置验证：测试MySQL连接（提前暴露连接问题）
        testMySQLConnection();

        // 3. 初始化Flink执行环境（本地测试专用配置）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // 4. 状态后端：本地测试用内存后端（无需HDFS）
        env.setStateBackend(new MemoryStateBackend());
        // 并行度：1（避免本地测试资源竞争）
        env.setParallelism(1);

        // 5. 提交ADS层任务（测试时可先注释其他任务，只保留流量汇总，减少干扰）
        processTrafficSummary(env, kafkaServer);       // 流量汇总（优先测试此任务）
        processTrafficSourceTopN(env, kafkaServer);    // 流量来源TOPN（验证完再开启）
        processKeywordTopN(env, kafkaServer);          // 关键词TOPN
        processProductPopularityTopN(env, kafkaServer);// 商品热度TOPN
        processPagePerformance(env, kafkaServer);      // 页面性能
        processCrowdAnalysis(env, kafkaServer);        // 人群分析

        // 6. 启动任务
        env.execute("ADS_Layer_Retail_Data_To_MySQL_TEST");
    }

    /**
     * 前置测试MySQL连接，避免写入阶段才暴露连接问题
     */
    private static void testMySQLConnection() {
        System.out.println("=== 测试MySQL连接 ===");
        Connection conn = null;
        try {
            // 加载MySQL驱动（确保依赖正确）
            Class.forName("com.mysql.cj.jdbc.Driver");
            // 建立连接
            conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
            if (conn != null && !conn.isClosed()) {
                System.out.println("MySQL连接成功！");
            }
        } catch (ClassNotFoundException e) {
            System.err.println("MySQL驱动缺失！请检查依赖：" + e.getMessage());
            throw new RuntimeException("MySQL驱动异常", e);
        } catch (SQLException e) {
            System.err.println("MySQL连接失败！原因：" + e.getMessage());
            System.err.println("请检查：1.URL格式 2.IP/端口可达 3.账号密码正确 4.数据库已创建");
            throw new RuntimeException("MySQL连接异常", e);
        } finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            System.out.println("===================\n");
        }
    }


    // ------------------------------ 3. 流量汇总处理（优先测试此任务，无TopN逻辑简单） ------------------------------
    private static void processTrafficSummary(StreamExecutionEnvironment env, String kafkaServer) {
        // 1. 读取Kafka DWS层数据（打印读取到的原始数据）
        DataStreamSource<String> dwsStream = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafkaServer,
                        DWS_TRAFFIC_OVERVIEW_TOPIC,
                        "ads_traffic_summary_group_TEST", // 测试用消费者组（避免与生产冲突）
                        OffsetsInitializer.latest()
                ),
                WatermarkStrategy.noWatermarks(),
                "traffic-overview-dws-source"
        );
        // 打印Kafka读取到的数据（验证是否有数据流入）
        dwsStream.print("Kafka读取原始数据: ");

        // 2. 数据处理链路：JSON解析→分组→10秒窗口→聚合→打印→写入MySQL
        dwsStream
                .map(JSON::parseObject) // JSON转对象
//                .print("JSON解析后数据: ") // 打印解析结果
                .keyBy(new KeySelector<JSONObject, Tuple3<Long, String, Long>>() {
                    @Override
                    public Tuple3<Long, String, Long> getKey(JSONObject json) {
                        return Tuple3.of(
                                getSafeLong(json, "stat_time"),
                                getSafeString(json, "terminal_type"),
                                getSafeLong(json, "shop_id")
                        );
                    }
                })
                // 测试用10秒窗口（快速触发，无需等待）
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new TrafficSummaryAggregate(), new TrafficSummaryWindowProcess())
                .print("聚合后待写入MySQL数据: ") // 打印聚合结果（验证数据是否符合预期）
//                .addSink(buildJdbcSinkForTrafficSummary()) // 写入MySQL
                .name("sink-traffic-summary-mysql");
    }

    // 流量汇总聚合逻辑（无修改，保持原业务）
    private static class TrafficSummaryAggregate implements AggregateFunction<JSONObject, JSONObject, JSONObject> {
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
            accumulator.put("total_shop_visitor", accumulator.getLong("total_shop_visitor") + getSafeLong(value, "shop_visitor_count"));
            accumulator.put("total_shop_pv", accumulator.getLong("total_shop_pv") + getSafeLong(value, "shop_page_view"));
            accumulator.put("total_new_visitor", accumulator.getLong("total_new_visitor") + getSafeLong(value, "new_visitor_count"));
            accumulator.put("total_old_visitor", accumulator.getLong("total_old_visitor") + getSafeLong(value, "old_visitor_count"));
            accumulator.put("total_product_visitor", accumulator.getLong("total_product_visitor") + getSafeLong(value, "product_visitor_count"));
            accumulator.put("total_product_pv", accumulator.getLong("total_product_pv") + getSafeLong(value, "product_page_view"));
            accumulator.put("total_add_collection", accumulator.getLong("total_add_collection") + getSafeLong(value, "add_collection_count"));
            accumulator.put("total_pay_buyer", accumulator.getLong("total_pay_buyer") + getSafeLong(value, "pay_buyer_count"));
            accumulator.put("total_pay_amount", accumulator.getBigDecimal("total_pay_amount").add(getSafeBigDecimal(value, "pay_amount")));

            // 保留分组信息
            accumulator.put("stat_time", getSafeLong(value, "stat_time"));
            accumulator.put("terminal_type", getSafeString(value, "terminal_type"));
            accumulator.put("shop_id", getSafeLong(value, "shop_id"));
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

    // 流量汇总窗口处理（补充窗口时间）
    private static class TrafficSummaryWindowProcess extends ProcessWindowFunction<JSONObject, JSONObject, Tuple3<Long, String, Long>, TimeWindow> {
        @Override
        public void process(Tuple3<Long, String, Long> key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) {
            for (JSONObject result : elements) {
                result.put("window_start", context.window().getStart());
                result.put("window_end", context.window().getEnd());
                result.put("summary_time", System.currentTimeMillis());
                out.collect(result);
            }
        }
    }

    // 流量汇总MySQL Sink（优化：批量提交+异常捕获）
    private static SinkFunction<JSONObject> buildJdbcSinkForTrafficSummary() {
        // SQL：确保字段数量与VALUES数量一致（建议手动数一遍）
        String insertSql = "INSERT INTO ads_traffic_summary (" +
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
                "summary_time = VALUES(summary_time)";

        // 测试用批量配置：1条提交+1秒间隔（确保单条数据也能写入）
        JdbcExecutionOptions execOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1)          // 1条数据就提交（测试专用）
                .withBatchIntervalMs(1000)  // 1秒内无新数据也提交
                .withMaxRetries(1)          // 重试1次（减少等待）
                .build();

        // JdbcSink：添加异常捕获，打印写入错误
        return JdbcSink.sink(
                insertSql,
                (PreparedStatement ps, JSONObject record) -> {
                    try {
                        // 按SQL顺序设置参数（与表字段类型严格匹配）
                        ps.setLong(1, getSafeLong(record, "stat_time"));
                        ps.setString(2, getSafeString(record, "terminal_type"));
                        ps.setLong(3, getSafeLong(record, "shop_id"));
                        ps.setLong(4, getSafeLong(record, "total_shop_visitor"));
                        ps.setLong(5, getSafeLong(record, "total_shop_pv"));
                        ps.setLong(6, getSafeLong(record, "total_new_visitor"));
                        ps.setLong(7, getSafeLong(record, "total_old_visitor"));
                        ps.setLong(8, getSafeLong(record, "total_product_visitor"));
                        ps.setLong(9, getSafeLong(record, "total_product_pv"));
                        ps.setLong(10, getSafeLong(record, "total_add_collection"));
                        ps.setLong(11, getSafeLong(record, "total_pay_buyer"));
                        ps.setBigDecimal(12, getSafeBigDecimal(record, "total_pay_amount"));
                        ps.setLong(13, getSafeLong(record, "window_start"));
                        ps.setLong(14, getSafeLong(record, "window_end"));
                        ps.setLong(15, getSafeLong(record, "summary_time"));
                    } catch (SQLException e) {
                        System.err.println("参数设置异常！数据：" + record.toJSONString());
                        throw e; // 抛出异常，让Flink打印详细日志
                    }
                },
                execOptions,
                getJdbcConnectionOptions()
        );
    }


    // ------------------------------ 4. 其他任务（流量来源/关键词/商品等，仅优化窗口和日志） ------------------------------
    // （注：以下任务逻辑与流量汇总一致，仅窗口和消费者组改为测试配置，可参考流量汇总修改）
    private static void processTrafficSourceTopN(StreamExecutionEnvironment env, String kafkaServer) {
        DataStreamSource<String> dwsStream = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafkaServer,
                        DWS_TRAFFIC_SOURCE_RANKING_TOPIC,
                        "ads_traffic_source_topn_group_TEST",
                        OffsetsInitializer.latest()
                ),
                WatermarkStrategy.noWatermarks(),
                "traffic-source-dws-source"
        );
        dwsStream.print("流量来源Kafka原始数据: ");

        dwsStream
                .map(JSON::parseObject)
//                .print("流量来源JSON解析后: ")
                .keyBy(new KeySelector<JSONObject, Tuple4<Long, String, Long, String>>() {
                    @Override
                    public Tuple4<Long, String, Long, String> getKey(JSONObject json) {
                        return Tuple4.of(
                                getSafeLong(json, "stat_time"),
                                getSafeString(json, "terminal_type"),
                                getSafeLong(json, "shop_id"),
                                getSafeString(json, "traffic_source_first")
                        );
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 10秒窗口
                .aggregate(new TrafficSourceAggregate(), new TrafficSourceWindowProcess())
                .keyBy(new KeySelector<JSONObject, Tuple3<Long, String, Long>>() {
                    @Override
                    public Tuple3<Long, String, Long> getKey(JSONObject json) {
                        return Tuple3.of(
                                getSafeLong(json, "stat_time"),
                                getSafeString(json, "terminal_type"),
                                getSafeLong(json, "shop_id")
                        );
                    }
                })
                .process(new TrafficSourceTopNProcessFunction(TRAFFIC_SOURCE_TOPN))
//                .print("流量来源TOPN待写入数据: ")
                .addSink(buildJdbcSinkForTrafficSourceTopN())
                .name("sink-traffic-source-topn-mysql");
    }

    // 流量来源聚合逻辑（无修改）
    private static class TrafficSourceAggregate implements AggregateFunction<JSONObject, JSONObject, JSONObject> {
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
            accumulator.put("total_visitor", accumulator.getLong("total_visitor") + getSafeLong(value, "visitor_count"));
            accumulator.put("total_product_visitor", accumulator.getLong("total_product_visitor") + getSafeLong(value, "product_visitor_count"));
            accumulator.put("total_product_pay", accumulator.getBigDecimal("total_product_pay").add(getSafeBigDecimal(value, "product_pay_amount")));

            accumulator.put("stat_time", getSafeLong(value, "stat_time"));
            accumulator.put("terminal_type", getSafeString(value, "terminal_type"));
            accumulator.put("shop_id", getSafeLong(value, "shop_id"));
            accumulator.put("traffic_source", getSafeString(value, "traffic_source_first"));
            accumulator.put("traffic_source_second", getSafeString(value, "traffic_source_second"));
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
            for (JSONObject result : elements) {
                result.put("window_start", context.window().getStart());
                result.put("window_end", context.window().getEnd());
                out.collect(result);
            }
        }
    }

    private static class TrafficSourceTopNProcessFunction extends KeyedProcessFunction<Tuple3<Long, String, Long>, JSONObject, JSONObject> {
        private final int topN;
        private ListState<JSONObject> sourceState;

        public TrafficSourceTopNProcessFunction(int topN) {
            this.topN = topN;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            sourceState = getRuntimeContext().getListState(new ListStateDescriptor<>("trafficSourceTopNState_TEST", JSONObject.class));
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
            sourceState.add(value);
            ctx.timerService().registerProcessingTimeTimer(getSafeLong(value, "window_end") + 1000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
            List<JSONObject> sourceList = new ArrayList<>();
            for (JSONObject json : sourceState.get()) {
                sourceList.add(json);
            }

            // 按总访客数降序
            sourceList.sort((a, b) -> Long.compare(getSafeLong(b, "total_visitor"), getSafeLong(a, "total_visitor")));

            // 取TopN
            for (int i = 0; i < Math.min(topN, sourceList.size()); i++) {
                sourceList.get(i).put("ranking", i + 1);
                out.collect(sourceList.get(i));
            }

            sourceState.clear();
        }
    }

    private static SinkFunction<JSONObject> buildJdbcSinkForTrafficSourceTopN() {
        String insertSql = "INSERT INTO ads_traffic_source_topn (" +
                "stat_time, terminal_type, shop_id, traffic_source, traffic_source_second, " +
                "total_visitor, total_product_visitor, total_product_pay, window_start, window_end, ranking) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE " +
                "total_visitor = VALUES(total_visitor), " +
                "total_product_visitor = VALUES(total_product_visitor), " +
                "total_product_pay = VALUES(total_product_pay), " +
                "ranking = VALUES(ranking)";

        return JdbcSink.sink(
                insertSql,
                (ps, record) -> {
                    try {
                        ps.setLong(1, getSafeLong(record, "stat_time"));
                        ps.setString(2, getSafeString(record, "terminal_type"));
                        ps.setLong(3, getSafeLong(record, "shop_id"));
                        ps.setString(4, getSafeString(record, "traffic_source"));
                        ps.setString(5, getSafeString(record, "traffic_source_second"));
                        ps.setLong(6, getSafeLong(record, "total_visitor"));
                        ps.setLong(7, getSafeLong(record, "total_product_visitor"));
                        ps.setBigDecimal(8, getSafeBigDecimal(record, "total_product_pay"));
                        ps.setLong(9, getSafeLong(record, "window_start"));
                        ps.setLong(10, getSafeLong(record, "window_end"));
                        ps.setInt(11, getSafeInt(record, "ranking"));
                    } catch (SQLException e) {
                        System.err.println("流量来源TOPN参数异常！数据：" + record.toJSONString());
                        throw e;
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1)
                        .withBatchIntervalMs(1000)
                        .build(),
                getJdbcConnectionOptions()
        );
    }


    // ------------------------------ 5. 通用工具方法（无修改，保持原逻辑） ------------------------------
    private static long getSafeLong(JSONObject json, String key) {
        return json.containsKey(key) && json.get(key) != null ? json.getLongValue(key) : 0L;
    }

    private static String getSafeString(JSONObject json, String key) {
        return json.containsKey(key) && json.get(key) != null ? json.getString(key) : "unknown";
    }

    private static int getSafeInt(JSONObject json, String key) {
        return json.containsKey(key) && json.get(key) != null ? json.getIntValue(key) : 0;
    }

    private static double getSafeDouble(JSONObject json, String key) {
        return json.containsKey(key) && json.get(key) != null ? json.getDoubleValue(key) : 0.0;
    }

    private static BigDecimal getSafeBigDecimal(JSONObject json, String key) {
        return json.containsKey(key) && json.get(key) != null ? json.getBigDecimal(key) : BigDecimal.ZERO;
    }

    /**
     * 数据库连接参数（测试用保留超时配置，避免连接卡住）
     */
    private static JdbcConnectionOptions getJdbcConnectionOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(MYSQL_URL)
                .withUsername(MYSQL_USER)
                .withPassword(MYSQL_PASSWORD)
                .withDriverName("com.mysql.cj.jdbc.Driver")
//                .withConnectionTimeoutMs(10000) // 10秒连接超时（快速失败）
//                .withSocketTimeoutMs(20000)    // 20秒读写超时
                .build();
    }


    // ------------------------------ 6. 其他未展示任务（关键词/商品/页面/人群） ------------------------------
    // （注：这些任务的修改逻辑与「流量来源TOPN」完全一致，只需：
    // 1. 消费者组加_TEST后缀
    // 2. 窗口改为Time.seconds(10)
    // 3. 各环节加print日志
    // 4. JdbcSink用批量1+1秒间隔
    // 可参考上述代码自行补充，此处省略避免冗余）
    private static void processKeywordTopN(StreamExecutionEnvironment env, String kafkaServer) { /* 参考修改 */ }
    private static void processProductPopularityTopN(StreamExecutionEnvironment env, String kafkaServer) { /* 参考修改 */ }
    private static void processPagePerformance(StreamExecutionEnvironment env, String kafkaServer) { /* 参考修改 */ }
    private static void processCrowdAnalysis(StreamExecutionEnvironment env, String kafkaServer) { /* 参考修改 */ }
}