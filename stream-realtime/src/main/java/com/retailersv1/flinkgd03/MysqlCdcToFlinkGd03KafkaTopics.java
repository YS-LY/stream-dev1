package com.retailersv1.flinkgd03;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.stream.utils.CdcSourceUtils;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashSet;
import java.util.Set;

/**
 * MySQL CDC采集FlinkGd03数据库的ODS层表数据，分别发送到不同的Kafka主题
 */
public class MysqlCdcToFlinkGd03KafkaTopics {

    private static final String KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String DATABASE_NAME = "FlinkGd03";

    // 为每张表定义对应的Kafka主题名称
    private static final String USER_VISIT_TOPIC = "FlinkGd03_ods_user_visit_log";
    private static final String PRODUCT_INTERACTION_TOPIC = "FlinkGd03_ods_product_interaction_log";
    private static final String ORDER_PAY_TOPIC = "FlinkGd03_ods_order_pay_log";
    private static final String SEARCH_TOPIC = "FlinkGd03_ods_search_log";
    private static final String PAGE_CLICK_TOPIC = "FlinkGd03_ods_page_click_log";
    private static final String CROWD_ATTRIBUTE_TOPIC = "FlinkGd03_ods_crowd_attribute_log";

    // 定义侧输出流标签用于新老用户分流
    private static final OutputTag<String> NEW_USER_TAG = new OutputTag<String>("new-user") {};
    private static final OutputTag<String> OLD_USER_TAG = new OutputTag<String>("old-user") {};

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setParallelism(1);

        // 创建MySQL CDC源，采集FlinkGd03数据库的所有表
        MySqlSource<String> mySqlSource = CdcSourceUtils.getMySQLCdcSource(
                DATABASE_NAME,
                DATABASE_NAME + ".*", // 采集所有表
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                "5000-6000",
                StartupOptions.initial()
        );

        // 读取CDC数据流
        DataStreamSource<String> cdcStream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "mysql-cdc-source"
        );

        // 处理数据流：打印原始数据、进行新老用户校验、按表名分发到不同Kafka主题
        SingleOutputStreamOperator<String> processedStream = cdcStream
                .map(jsonStr -> {
                    // 打印原始数据查看结构
                    System.out.println("Original CDC Data: " + jsonStr);
                    return JSONObject.parseObject(jsonStr);
                })
                .process(new ProcessFunction<JSONObject, String>() {
                    // 存储已知访客ID的集合（实际生产环境中应使用外部存储如Redis）
                    private Set<String> visitorIds = new HashSet<>();

                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                        // 获取表名
                        String tableName = jsonObject.getJSONObject("source").getString("table");

                        // 提取数据部分
                        JSONObject data;
                        String op = jsonObject.getString("op");

                        switch (op) {
                            case "r": // 读取快照数据
                            case "c": // 插入操作
                                data = jsonObject.getJSONObject("after");
                                break;
                            case "u": // 更新操作
                                data = jsonObject.getJSONObject("after");
                                break;
                            case "d": // 删除操作
                                data = jsonObject.getJSONObject("before");
                                break;
                            default:
                                return;
                        }

                        // 对于包含visitor_id的表，进行新老用户校验
                        if (data.containsKey("visitor_id")) {
                            String visitorId = data.getString("visitor_id");
                            boolean isNewVisitor = !visitorIds.contains(visitorId); // 判断是否为新访客

                            // 更新访客集合
                            visitorIds.add(visitorId);

                            // 更新is_new_visitor字段（如果存在该字段）
                            if (data.containsKey("is_new_visitor")) {
                                data.put("is_new_visitor", isNewVisitor ? 1 : 0);

                                // 根据新老用户分流到侧输出流
                                if (isNewVisitor) {
                                    context.output(NEW_USER_TAG, data.toJSONString());
                                } else {
                                    context.output(OLD_USER_TAG, data.toJSONString());
                                }
                            }
                        }

                        // 添加表名标识到数据中
                        data.put("_table", tableName);

                        // 发送到主输出流
                        collector.collect(data.toJSONString());

                        // 根据表名分发到不同的Kafka主题
                        String topicName = getTopicNameByTable(tableName);
                        if (topicName != null) {
                            // 这里只是示例，实际分发在sink部分处理
                            // 可以通过其他方式实现分发表功能
                        }
                    }

                    private String getTopicNameByTable(String tableName) {
                        switch (tableName) {
                            case "ods_user_visit_log":
                                return USER_VISIT_TOPIC;
                            case "ods_product_interaction_log":
                                return PRODUCT_INTERACTION_TOPIC;
                            case "ods_order_pay_log":
                                return ORDER_PAY_TOPIC;
                            case "ods_search_log":
                                return SEARCH_TOPIC;
                            case "ods_page_click_log":
                                return PAGE_CLICK_TOPIC;
                            case "ods_crowd_attribute_log":
                                return CROWD_ATTRIBUTE_TOPIC;
                            default:
                                return null;
                        }
                    }
                });

        // 按表名分发数据到不同的Kafka主题
        processedStream
                .map(jsonStr -> {
                    JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                    return jsonObject;
                })
                .addSink(new org.apache.flink.streaming.api.functions.sink.SinkFunction<JSONObject>() {
                    @Override
                    public void invoke(JSONObject value, Context context) throws Exception {
                        // 实际项目中应使用更完善的分发逻辑
                        System.out.println("Processing data: " + value.toJSONString());
                    }
                });

        // 为每张表创建独立的数据流并发送到对应的Kafka主题
        // ods_user_visit_log 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "ods_user_visit_log".equals(tableName);
                    } catch (Exception e) {
                        System.err.println("Error parsing JSON: " + jsonStr);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    // 可以在这里进行特定处理
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, USER_VISIT_TOPIC))
                .name("sink-user-visit-to-kafka");

        // ods_product_interaction_log 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "ods_product_interaction_log".equals(tableName);
                    } catch (Exception e) {
                        System.err.println("Error parsing JSON: " + jsonStr);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, PRODUCT_INTERACTION_TOPIC))
                .name("sink-product-interaction-to-kafka");

        // ods_order_pay_log 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "ods_order_pay_log".equals(tableName);
                    } catch (Exception e) {
                        System.err.println("Error parsing JSON: " + jsonStr);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, ORDER_PAY_TOPIC))
                .name("sink-order-pay-to-kafka");

        // ods_search_log 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "ods_search_log".equals(tableName);
                    } catch (Exception e) {
                        System.err.println("Error parsing JSON: " + jsonStr);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, SEARCH_TOPIC))
                .name("sink-search-to-kafka");

        // ods_page_click_log 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "ods_page_click_log".equals(tableName);
                    } catch (Exception e) {
                        System.err.println("Error parsing JSON: " + jsonStr);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, PAGE_CLICK_TOPIC))
                .name("sink-page-click-to-kafka");

        // ods_crowd_attribute_log 表数据
        processedStream
                .filter(jsonStr -> {
                    if (jsonStr == null) {
                        return false;
                    }
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                        String tableName = jsonObject.getString("_table");
                        return "ods_crowd_attribute_log".equals(tableName);
                    } catch (Exception e) {
                        System.err.println("Error parsing JSON: " + jsonStr);
                        return false;
                    }
                })
                .map(JSONObject::parseObject)
                .map(jsonObj -> {
                    // 移除表名标识字段，恢复原始数据格式
                    jsonObj.remove("_table");
                    return jsonObj.toJSONString();
                })
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, CROWD_ATTRIBUTE_TOPIC))
                .name("sink-crowd-attribute-to-kafka");

        // 所有数据发送到统一主题
//        processedStream
//                .map(jsonStr -> {
//                    // 移除表名标识字段，恢复原始数据格式
//                    JSONObject jsonObj = JSONObject.parseObject(jsonStr);
//                    jsonObj.remove("_table");
//                    return jsonObj.toJSONString();
//                })
//                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, "FlinkGd03_all_data"))
//                .name("sink-all-data-to-kafka");

        // 获取侧输出流并发送到对应主题
//        SideOutputDataStream<String> newUserStream = processedStream.getSideOutput(NEW_USER_TAG);
//        SideOutputDataStream<String> oldUserStream = processedStream.getSideOutput(OLD_USER_TAG);

//        newUserStream
//                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, "FlinkGd03_new_users"))
//                .name("sink-new-users-to-kafka");

//        oldUserStream
//                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, "FlinkGd03_old_users"))
//                .name("sink-old-users-to-kafka");

        env.execute("MySQL CDC to FlinkGd03 Kafka Topics");
    }
}
