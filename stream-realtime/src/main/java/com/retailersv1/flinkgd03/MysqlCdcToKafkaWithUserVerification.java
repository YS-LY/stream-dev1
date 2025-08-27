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
 * @BelongsProject: zxk_git
 * @BelongsPackage: com.flinkgd03.ods
 * @Author: zhuxiangkuan
 * @CreateTime: 2025-08-27  09:45
 * @Description: TODO MySQL CDC采集FlinkGd03数据库的ODS层数据，进行新老用户校验后发送到Kafka
 * @Version: 1.0
 */

public class MysqlCdcToKafkaWithUserVerification {

    private static final String KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String DATABASE_NAME = "FlinkGd03";

    // 定义侧输出流标签
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
                StartupOptions.earliest()
        );

        // 读取CDC数据流
        DataStreamSource<String> cdcStream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "mysql-cdc-source"
        );

        // 处理数据流：打印原始数据、进行新老用户校验、分流发送到Kafka
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

                        // 对于ODS层的用户行为表，检查is_new_visitor字段并进行校验
                        if (data.containsKey("visitor_id")) {
                            String visitorId = data.getString("visitor_id");
                            boolean isNewVisitor = visitorIds.add(visitorId); // 如果添加成功，说明是新访客

                            // 更新is_new_visitor字段
                            if (isNewVisitor) {
                                data.put("is_new_visitor", 1);
                                context.output(NEW_USER_TAG, data.toJSONString());
                            } else {
                                data.put("is_new_visitor", 0);
                                context.output(OLD_USER_TAG, data.toJSONString());
                            }
                        }

                        // 发送到主输出流
                        collector.collect(data.toJSONString());
                    }
                });

        // 获取侧输出流
        SideOutputDataStream<String> newUserStream = processedStream.getSideOutput(NEW_USER_TAG);
        SideOutputDataStream<String> oldUserStream = processedStream.getSideOutput(OLD_USER_TAG);

        // 将处理后的数据发送到不同的Kafka主题
        processedStream
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, "FlinkGd03_ods_all_data"))
                .name("sink-all-ods-data-to-kafka");
        processedStream.print();

//        newUserStream
//                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, "ods_new_user_data"))
//                .name("sink-new-user-data-to-kafka");
//        newUserStream.print();
//        oldUserStream
//                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_SERVER, "ods_old_user_data"))
//                .name("sink-old-user-data-to-kafka");
//        oldUserStream.print();
        env.execute("MySQL CDC to Kafka with User Verification");
    }
}
