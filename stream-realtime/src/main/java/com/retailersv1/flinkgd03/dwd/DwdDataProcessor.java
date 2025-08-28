package com.retailersv1.flinkgd03.dwd;




import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @BelongsProject: zxk_git
 * @BelongsPackage: com.flinkgd03.dwd
 * @Author: zhuxiangkuan
 * @CreateTime: 2025-08-28  16:34
 * @Description: TODO DWD层数据处理：从ODS层Kafka主题读取数据，处理后写入DWD层Kafka主题
 * @Version: 1.0
 */

public class DwdDataProcessor {

    // 定义DWD层Kafka主题
    private static final String DWD_USER_VISIT_TOPIC = "FlinkGd03_dwd_user_visit_detail";
    private static final String DWD_PRODUCT_INTERACTION_TOPIC = "FlinkGd03_dwd_product_interaction_detail";
    private static final String DWD_PAY_TOPIC = "FlinkGd03_dwd_pay_detail";
    private static final String DWD_SEARCH_TOPIC = "FlinkGd03_dwd_search_detail";
    private static final String DWD_PAGE_CLICK_TOPIC = "FlinkGd03_dwd_page_click_detail";
    private static final String DWD_CROWD_ATTRIBUTE_TOPIC = "FlinkGd03_dwd_crowd_attribute_detail";

    // 定义侧输出流标签
    private static final OutputTag<String> USER_VISIT_TAG = new OutputTag<String>("user-visit") {};
    private static final OutputTag<String> PRODUCT_INTERACTION_TAG = new OutputTag<String>("product-interaction") {};
    private static final OutputTag<String> PAY_TAG = new OutputTag<String>("pay") {};
    private static final OutputTag<String> SEARCH_TAG = new OutputTag<String>("search") {};
    private static final OutputTag<String> PAGE_CLICK_TAG = new OutputTag<String>("page-click") {};
    private static final OutputTag<String> CROWD_ATTRIBUTE_TAG = new OutputTag<String>("crowd-attribute") {};

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setParallelism(1);

        String kafkaServer = ConfigUtils.getString("kafka.bootstrap.servers");

        // 读取各个ODS层主题数据
        DataStreamSource<String> userVisitStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_ods_user_visit_log", "dwd_user_visit_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "user-visit-source"
        );

        DataStreamSource<String> productInteractionStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_ods_product_interaction_log", "dwd_product_interaction_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "product-interaction-source"
        );

        DataStreamSource<String> payStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_ods_order_pay_log", "dwd_pay_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "pay-source"
        );

        DataStreamSource<String> searchStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_ods_search_log", "dwd_search_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "search-source"
        );

        DataStreamSource<String> pageClickStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_ods_page_click_log", "dwd_page_click_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "page-click-source"
        );

        DataStreamSource<String> crowdAttributeStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_ods_crowd_attribute_log", "dwd_crowd_attribute_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "crowd-attribute-source"
        );

        // 处理用户访问数据
        userVisitStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 数据清洗：过滤空visitor_id、异常visit_time
                            if (jsonObject.getString("visitor_id") == null ||
                                    jsonObject.getString("visitor_id").trim().isEmpty() ||
                                    jsonObject.getLong("visit_time") == null) {
                                return;
                            }

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("log_id", jsonObject.getLong("log_id"));
                            dwdData.put("visit_time", jsonObject.getLong("visit_time"));
                            dwdData.put("visitor_id", jsonObject.getString("visitor_id"));
                            dwdData.put("terminal_type", jsonObject.getString("terminal_type"));
                            dwdData.put("visit_object_type", jsonObject.getString("visit_object_type"));
                            dwdData.put("visit_object_id", jsonObject.getLong("visit_object_id"));
                            dwdData.put("traffic_source_first", jsonObject.getString("traffic_source_first"));
                            dwdData.put("traffic_source_second", jsonObject.getString("traffic_source_second"));
                            dwdData.put("is_new_visitor", jsonObject.getInteger("is_new_visitor"));
                            dwdData.put("page_view", jsonObject.getInteger("page_view"));
                            dwdData.put("stay_time", jsonObject.getInteger("stay_time"));

                            // 计算stat_time字段
                            long visitTime = jsonObject.getLong("visit_time");
                            LocalDateTime visitDateTime = LocalDateTime.ofEpochSecond(visitTime/1000, 0, java.time.ZoneOffset.UTC);
                            LocalDateTime statTime = visitDateTime.withMinute(0).withSecond(0).withNano(0);
                            dwdData.put("stat_time", statTime.atZone(java.time.ZoneId.systemDefault()).toEpochSecond() * 1000);

                            // 添加shop_id字段（示例值，实际应根据业务逻辑确定）
                            dwdData.put("shop_id", 10001L);

                            ctx.output(USER_VISIT_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理用户访问数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(USER_VISIT_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_USER_VISIT_TOPIC))
                .name("sink-user-visit-dwd");

        // 处理商品交互数据
        productInteractionStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 数据清洗：过滤is_cancel=1的数据
                            if (jsonObject.getInteger("is_cancel") != null &&
                                    jsonObject.getInteger("is_cancel") == 1) {
                                return;
                            }

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("log_id", jsonObject.getLong("log_id"));
                            dwdData.put("interact_time", jsonObject.getLong("interact_time"));
                            dwdData.put("visitor_id", jsonObject.getString("visitor_id"));
                            dwdData.put("terminal_type", jsonObject.getString("terminal_type"));
                            dwdData.put("product_id", jsonObject.getLong("product_id"));
                            dwdData.put("interact_type", jsonObject.getString("interact_type"));

                            // 计算stat_time字段
                            long interactTime = jsonObject.getLong("interact_time");
                            LocalDateTime interactDateTime = LocalDateTime.ofEpochSecond(interactTime/1000, 0, java.time.ZoneOffset.UTC);
                            LocalDateTime statTime = interactDateTime.withMinute(0).withSecond(0).withNano(0);
                            dwdData.put("stat_time", statTime.atZone(java.time.ZoneId.systemDefault()).toEpochSecond() * 1000);

                            // 添加shop_id字段（示例值，实际应根据业务逻辑确定）
                            dwdData.put("shop_id", 10001L);

                            ctx.output(PRODUCT_INTERACTION_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理商品交互数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(PRODUCT_INTERACTION_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_PRODUCT_INTERACTION_TOPIC))
                .name("sink-product-interaction-dwd");

        // 处理支付数据
        payStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 数据清洗：只处理已支付订单
                            if (jsonObject.getInteger("is_paid") == null ||
                                    jsonObject.getInteger("is_paid") != 1) {
                                return;
                            }

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("log_id", jsonObject.getLong("log_id"));
                            dwdData.put("pay_time", jsonObject.getLong("pay_time"));
                            dwdData.put("buyer_id", jsonObject.getString("buyer_id"));
                            dwdData.put("terminal_type", jsonObject.getString("terminal_type"));
                            dwdData.put("order_id", jsonObject.getLong("order_id"));
                            dwdData.put("product_id", jsonObject.getLong("product_id"));
                            dwdData.put("pay_amount", jsonObject.getString("pay_amount"));

                            // 计算stat_time字段
                            long payTime = jsonObject.getLong("pay_time");
                            LocalDateTime payDateTime = LocalDateTime.ofEpochSecond(payTime/1000, 0, java.time.ZoneOffset.UTC);
                            LocalDateTime statTime = payDateTime.withMinute(0).withSecond(0).withNano(0);
                            dwdData.put("stat_time", statTime.atZone(java.time.ZoneId.systemDefault()).toEpochSecond() * 1000);

                            // 添加pay_quantity字段（示例值，实际应根据业务逻辑确定）
                            dwdData.put("pay_quantity", 1);

                            // 添加shop_id字段（示例值，实际应根据业务逻辑确定）
                            dwdData.put("shop_id", 10001L);

                            ctx.output(PAY_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理支付数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(PAY_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_PAY_TOPIC))
                .name("sink-pay-dwd");

        // 处理搜索数据
        searchStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("log_id", jsonObject.getLong("log_id"));
                            dwdData.put("search_time", jsonObject.getLong("search_time"));
                            dwdData.put("visitor_id", jsonObject.getString("visitor_id"));
                            dwdData.put("terminal_type", jsonObject.getString("terminal_type"));
                            dwdData.put("keyword", jsonObject.getString("keyword"));
                            dwdData.put("is_click_result", jsonObject.getInteger("is_click_result"));
                            dwdData.put("click_product_id", jsonObject.getLong("click_product_id"));

                            // 计算stat_time字段（日期级别）
                            long searchTime = jsonObject.getLong("search_time");
                            LocalDateTime searchDateTime = LocalDateTime.ofEpochSecond(searchTime/1000, 0, java.time.ZoneOffset.UTC);
                            LocalDateTime statTime = searchDateTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
                            dwdData.put("stat_time", statTime.atZone(java.time.ZoneId.systemDefault()).toEpochSecond() * 1000);

                            // 添加stat_dimension字段
                            dwdData.put("stat_dimension", "day");

                            // 添加shop_id字段（示例值，实际应根据业务逻辑确定）
                            dwdData.put("shop_id", 10001L);

                            ctx.output(SEARCH_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理搜索数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(SEARCH_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_SEARCH_TOPIC))
                .name("sink-search-dwd");

        // 处理页面点击数据
        pageClickStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("log_id", jsonObject.getLong("log_id"));
                            dwdData.put("click_time", jsonObject.getLong("click_time"));
                            dwdData.put("visitor_id", jsonObject.getString("visitor_id"));
                            dwdData.put("terminal_type", jsonObject.getString("terminal_type"));
                            dwdData.put("page_id", jsonObject.getLong("page_id"));
                            dwdData.put("module_name", jsonObject.getString("module_name"));
                            dwdData.put("module_position", jsonObject.getString("module_position"));
                            dwdData.put("guide_product_id", jsonObject.getLong("guide_product_id"));

                            // 计算stat_time字段
                            long clickTime = jsonObject.getLong("click_time");
                            LocalDateTime clickDateTime = LocalDateTime.ofEpochSecond(clickTime/1000, 0, java.time.ZoneOffset.UTC);
                            LocalDateTime statTime = clickDateTime.withMinute(0).withSecond(0).withNano(0);
                            dwdData.put("stat_time", statTime.atZone(java.time.ZoneId.systemDefault()).toEpochSecond() * 1000);

                            // 添加shop_id字段（示例值，实际应根据业务逻辑确定）
                            dwdData.put("shop_id", 10001L);

                            ctx.output(PAGE_CLICK_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理页面点击数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(PAGE_CLICK_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_PAGE_CLICK_TOPIC))
                .name("sink-page-click-dwd");

        // 处理人群属性数据
        crowdAttributeStream.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);

                            // 构建DWD层数据
                            JSONObject dwdData = new JSONObject();
                            dwdData.put("log_id", jsonObject.getLong("log_id"));
                            dwdData.put("visitor_id", jsonObject.getString("visitor_id"));
                            dwdData.put("gender", jsonObject.getString("gender"));
                            dwdData.put("age_range", jsonObject.getString("age_range"));
                            dwdData.put("city", jsonObject.getString("city"));
                            dwdData.put("taoqi_value", jsonObject.getInteger("taoqi_value"));
                            dwdData.put("update_time", jsonObject.getLong("update_time"));

                            // 计算stat_time字段
                            long updateTime = jsonObject.getLong("update_time");
                            LocalDateTime updateDateTime = LocalDateTime.ofEpochSecond(updateTime/1000, 0, java.time.ZoneOffset.UTC);
                            LocalDateTime statTime = updateDateTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
                            dwdData.put("stat_time", statTime.atZone(java.time.ZoneId.systemDefault()).toEpochSecond() * 1000);

                            // 添加shop_id字段（示例值，实际应根据业务逻辑确定）
                            dwdData.put("shop_id", 10001L);

                            ctx.output(CROWD_ATTRIBUTE_TAG, dwdData.toJSONString());
                        } catch (Exception e) {
                            System.err.println("处理人群属性数据异常: " + value + ", 错误: " + e.getMessage());
                        }
                    }
                }).getSideOutput(CROWD_ATTRIBUTE_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServer, DWD_CROWD_ATTRIBUTE_TOPIC))
                .name("sink-crowd-attribute-dwd");

        env.execute("DWD Layer Data Processing");
    }
}

