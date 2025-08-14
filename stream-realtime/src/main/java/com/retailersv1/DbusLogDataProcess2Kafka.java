package com.retailersv1;

import com.stream.common.utils.CommonUtils;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import com.alibaba.fastjson.JSONObject;

import java.util.Date;
import java.util.HashMap;

public class DbusLogDataProcess2Kafka {

    private static final String kafka_topic_base_log_data = ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC");
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_err_log = ConfigUtils.getString("kafka.err.log");
    private static final String kafka_start_log = ConfigUtils.getString("kafka.start.log");
    private static final String kafka_display_log = ConfigUtils.getString("kafka.display.log");
    private static final String kafka_action_log = ConfigUtils.getString("kafka.action.log");
    private static final String kafka_dirty_topic = ConfigUtils.getString("kafka.dirty.topic");
    private static final String kafka_page_topic = ConfigUtils.getString("kafka.page.topic");
    private static final OutputTag<String> errTag = new OutputTag<String>("errTag") {};
    private static final OutputTag<String> startTag = new OutputTag<String>("startTag") {};
    private static final OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
    private static final OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
    private static final OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {};
    private static final HashMap<String,DataStream<String>> collectDsMap = new HashMap<>();

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        CommonUtils.printCheckPropEnv(
                false,
                kafka_topic_base_log_data,
                kafka_botstrap_servers,
                kafka_page_topic,
                kafka_err_log,
                kafka_start_log,
                kafka_display_log,
                kafka_action_log,
                kafka_dirty_topic
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_botstrap_servers,
                        kafka_topic_base_log_data,
                        new Date().toString(),
                        OffsetsInitializer.latest()
                ),
                WatermarkStrategy.noWatermarks(),
                "read_kafka_realtime_log"
        );


        SingleOutputStreamOperator<JSONObject> processDS = kafkaSourceDs.process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) {
                        try {
                           collector.collect(JSONObject.parseObject(s));
                        } catch (Exception e) {
                            context.output(dirtyTag, s);
                            System.err.println("Convert JsonData Error !");
                        }
                    }
                }).uid("convert_json_process")
                .name("convert_json_process");

        SideOutputDataStream<String> dirtyDS = processDS.getSideOutput(dirtyTag);
        dirtyDS.print("dirtyDS -> ");
        processDS.print();
        dirtyDS.sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers,kafka_dirty_topic))
                .uid("sink_dirty_data_to_kafka")
                .name("sink_dirty_data_to_kafka");
// 示例：如果使用了HDFS作为检查点路径
        env.setStateBackend(new FsStateBackend("file:///tmp/flink/checkpoints"));

        env.execute();
    }
}
