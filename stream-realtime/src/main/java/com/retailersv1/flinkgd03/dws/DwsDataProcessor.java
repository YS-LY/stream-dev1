package com.retailersv1.flinkgd03.dws;




import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;



public class DwsDataProcessor {

    // 定义DWS层Kafka主题
    private static final String DWS_TRAFFIC_OVERVIEW_TOPIC = "FlinkGd03_dws_traffic_overview";
    private static final String DWS_TRAFFIC_SOURCE_RANKING_TOPIC = "FlinkGd03_dws_traffic_source_ranking";
    private static final String DWS_KEYWORD_RANKING_TOPIC = "FlinkGd03_dws_keyword_ranking";
    private static final String DWS_PRODUCT_TRAFFIC_TOPIC = "FlinkGd03_dws_product_traffic";
    private static final String DWS_PAGE_TRAFFIC_TOPIC = "FlinkGd03_dws_page_traffic";
    private static final String DWS_CROWD_FEATURE_TOPIC = "FlinkGd03_dws_crowd_feature";

    // 定义侧输出流标签
    private static final OutputTag<String> TRAFFIC_OVERVIEW_TAG = new OutputTag<String>("traffic-overview") {};
    private static final OutputTag<String> TRAFFIC_SOURCE_RANKING_TAG = new OutputTag<String>("traffic-source-ranking") {};
    private static final OutputTag<String> KEYWORD_RANKING_TAG = new OutputTag<String>("keyword-ranking") {};
    private static final OutputTag<String> PRODUCT_TRAFFIC_TAG = new OutputTag<String>("product-traffic") {};
    private static final OutputTag<String> PAGE_TRAFFIC_TAG = new OutputTag<String>("page-traffic") {};
    private static final OutputTag<String> CROWD_FEATURE_TAG = new OutputTag<String>("crowd-feature") {};

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setParallelism(1);

        String kafkaServer = ConfigUtils.getString("kafka.bootstrap.servers");

        // 读取各个DWD层主题数据
        DataStreamSource<String> userVisitStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_dwd_user_visit_detail", "dws_user_visit_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "user-visit-dwd-source"
        );

        DataStreamSource<String> productInteractionStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_dwd_product_interaction_detail", "dws_product_interaction_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "product-interaction-dwd-source"
        );

        DataStreamSource<String> payStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_dwd_pay_detail", "dws_pay_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "pay-dwd-source"
        );

        DataStreamSource<String> searchStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_dwd_search_detail", "dws_search_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "search-dwd-source"
        );

        DataStreamSource<String> pageClickStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_dwd_page_click_detail", "dws_page_click_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "page-click-dwd-source"
        );

        DataStreamSource<String> crowdAttributeStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_dwd_crowd_attribute_detail", "dws_crowd_attribute_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "crowd-attribute-dwd-source"
        );

        // 处理流量总览数据
        userVisitStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 构建DWS层流量总览数据
                            JSONObject dwsData = new JSONObject();
                            dwsData.put("stat_time", jsonObject.getLong("stat_time"));
                            dwsData.put("terminal_type", jsonObject.getString("terminal_type"));
                            dwsData.put("shop_id", jsonObject.getLong("shop_id"));

                            // 访问店铺指标
                            if ("shop".equals(jsonObject.getString("visit_object_type"))) {
                                dwsData.put("shop_visitor_count", 1);
                                dwsData.put("shop_page_view", jsonObject.getInteger("page_view"));
                                dwsData.put("new_visitor_count", jsonObject.getInteger("is_new_visitor") == 1 ? 1 : 0);
                                dwsData.put("old_visitor_count", jsonObject.getInteger("is_new_visitor") == 1 ? 0 : 1);
                            } else {
                                dwsData.put("shop_visitor_count", 0);
                                dwsData.put("shop_page_view", 0);
                                dwsData.put("new_visitor_count", 0);
                                dwsData.put("old_visitor_count", 0);
                            }

                            // 访问商品指标
                            if ("product".equals(jsonObject.getString("visit_object_type"))) {
                                dwsData.put("product_visitor_count", 1);
                                dwsData.put("product_page_view", jsonObject.getInteger("page_view"));
                            } else {
                                dwsData.put("product_visitor_count", 0);
                                dwsData.put("product_page_view", 0);
                            }

                            // 初始化其他指标为0
                            dwsData.put("add_collection_count", 0);
                            dwsData.put("pay_buyer_count", 0);
                            dwsData.put("pay_amount", BigDecimal.ZERO);

                            ctx.output(TRAFFIC_OVERVIEW_TAG, dwsData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理流量总览数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(TRAFFIC_OVERVIEW_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWS_TRAFFIC_OVERVIEW_TOPIC))
                .name("sink-traffic-overview-dws");

        // 处理流量来源排行数据
        userVisitStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 构建DWS层流量来源排行数据
                            JSONObject dwsData = new JSONObject();
                            dwsData.put("stat_time", jsonObject.getLong("stat_time"));
                            dwsData.put("terminal_type", jsonObject.getString("terminal_type"));
                            dwsData.put("shop_id", jsonObject.getLong("shop_id"));
                            dwsData.put("traffic_source_first", jsonObject.getString("traffic_source_first"));
                            dwsData.put("traffic_source_second", jsonObject.getString("traffic_source_second"));
                            dwsData.put("visitor_count", 1);

                            // 初始化其他字段
                            dwsData.put("sub_source_list", new JSONArray());
                            dwsData.put("related_product_id", jsonObject.getLong("visit_object_id"));
                            dwsData.put("product_visitor_count", "product".equals(jsonObject.getString("visit_object_type")) ? 1 : 0);
                            dwsData.put("product_pay_amount", BigDecimal.ZERO);

                            ctx.output(TRAFFIC_SOURCE_RANKING_TAG, dwsData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理流量来源排行数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(TRAFFIC_SOURCE_RANKING_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWS_TRAFFIC_SOURCE_RANKING_TOPIC))
                .name("sink-traffic-source-ranking-dws");

        // 处理关键词排行数据
        searchStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 构建DWS层关键词排行数据
                            JSONObject dwsData = new JSONObject();
                            dwsData.put("stat_time", jsonObject.getString("stat_time").substring(0, 10)); // 只取日期部分
                            dwsData.put("stat_dimension", jsonObject.getString("stat_dimension"));
                            dwsData.put("terminal_type", jsonObject.getString("terminal_type"));
                            dwsData.put("shop_id", jsonObject.getLong("shop_id"));
                            dwsData.put("keyword", jsonObject.getString("keyword"));
                            dwsData.put("search_visitor_count", 1);
                            dwsData.put("click_visitor_count", jsonObject.getInteger("is_click_result"));

                            ctx.output(KEYWORD_RANKING_TAG, dwsData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理关键词排行数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(KEYWORD_RANKING_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWS_KEYWORD_RANKING_TOPIC))
                .name("sink-keyword-ranking-dws");

        // 处理商品流量数据
        userVisitStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 只处理访问商品的数据
                            if (!"product".equals(jsonObject.getString("visit_object_type"))) {
                                return;
                            }

                            // 构建DWS层商品流量数据
                            JSONObject dwsData = new JSONObject();
                            dwsData.put("stat_time", jsonObject.getLong("stat_time"));
                            dwsData.put("terminal_type", jsonObject.getString("terminal_type"));
                            dwsData.put("shop_id", jsonObject.getLong("shop_id"));
                            dwsData.put("product_id", jsonObject.getLong("visit_object_id"));
                            dwsData.put("visitor_count", 1);
                            dwsData.put("page_view", jsonObject.getInteger("page_view"));
                            dwsData.put("avg_stay_time", jsonObject.getInteger("stay_time"));

                            // 初始化其他指标
                            dwsData.put("add_cart_count", 0);
                            dwsData.put("collection_count", 0);
                            dwsData.put("pay_buyer_count", 0);
                            dwsData.put("pay_amount", BigDecimal.ZERO);
                            dwsData.put("traffic_source_json", new JSONArray());

                            ctx.output(PRODUCT_TRAFFIC_TAG, dwsData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理商品流量数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(PRODUCT_TRAFFIC_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWS_PRODUCT_TRAFFIC_TOPIC))
                .name("sink-product-traffic-dws");

        // 处理页面流量数据
        userVisitStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 只处理访问页面的数据
                            if (!"page".equals(jsonObject.getString("visit_object_type"))) {
                                return;
                            }

                            // 构建DWS层页面流量数据
                            JSONObject dwsData = new JSONObject();
                            dwsData.put("stat_time", jsonObject.getLong("stat_time"));
                            dwsData.put("terminal_type", jsonObject.getString("terminal_type"));
                            dwsData.put("shop_id", jsonObject.getLong("shop_id"));
                            dwsData.put("page_id", jsonObject.getLong("visit_object_id"));
                            dwsData.put("visitor_count", 1);
                            dwsData.put("page_view", jsonObject.getInteger("page_view"));
                            dwsData.put("click_count", 0); // 点击数需要与页面点击数据关联
                            dwsData.put("avg_stay_time", jsonObject.getInteger("stay_time"));
                            dwsData.put("module_click_json", new JSONArray());
                            dwsData.put("guide_product_json", new JSONArray());

                            ctx.output(PAGE_TRAFFIC_TAG, dwsData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理页面流量数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(PAGE_TRAFFIC_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWS_PAGE_TRAFFIC_TOPIC))
                .name("sink-page-traffic-dws");

        // 处理人群特征数据
        crowdAttributeStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 构建DWS层人群特征数据（不同人群类型）
                            String[] crowdTypes = {"shop_entry", "product_visit", "conversion"};

                            for (String crowdType : crowdTypes) {
                                JSONObject dwsData = new JSONObject();
                                dwsData.put("stat_time", jsonObject.getLong("stat_time"));
                                dwsData.put("terminal_type", "overall"); // 默认终端类型
                                dwsData.put("shop_id", jsonObject.getLong("shop_id"));
                                dwsData.put("crowd_type", crowdType);
                                dwsData.put("gender", jsonObject.getString("gender"));
                                dwsData.put("age_range", jsonObject.getString("age_range"));
                                dwsData.put("city", jsonObject.getString("city"));
                                dwsData.put("taoqi_value", jsonObject.getInteger("taoqi_value"));
                                dwsData.put("crowd_count", 1);
                                dwsData.put("total_crowd_count", 1); // 需要根据实际数据计算
                                dwsData.put("crowd_ratio", new BigDecimal("1.0000"));

                                ctx.output(CROWD_FEATURE_TAG, dwsData.toJSONString());
                            }
                        } catch (Exception e) {
                            System.err.println("处理人群特征数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(CROWD_FEATURE_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWS_CROWD_FEATURE_TOPIC))
                .name("sink-crowd-feature-dws");

        env.execute("DWS Layer Data Processing");
    }
}

