package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.MapUpdateHbaseDimTableFunc;
import com.retailersv1.func.ProcessSpiltStreamToHBaseDimFunc;
import com.retailersv1.stream.utils.CdcSourceUtils;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DbusCdc2DimHbaseAnd2DbKafka {
    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String CDH_KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    private static final String MYSQL_CDC_TO_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                "5403-5450",
                StartupOptions.earliest()
        );
        MySqlSource<String> mySQLCdcDimConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "realtime_v1_config.table_process_dim",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                "2000-2098",
                StartupOptions.earliest()
        );

        DataStreamSource<String> cdcDbMainStream = env.fromSource(mySQLDbMainCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_main_source");
        DataStreamSource<String> cdcDbDimStream = env.fromSource(mySQLCdcDimConfSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_dim_source");

        SingleOutputStreamOperator<JSONObject> cdcDbMainStreamMap = cdcDbMainStream.map(JSONObject::parseObject)
                .uid("db_data_convert_json")
                .name("db_data_convert_json")
                .setParallelism(1);

        cdcDbMainStreamMap.map(JSONObject::toString)
                .sinkTo(
                        KafkaUtils.buildKafkaSink(CDH_KAFKA_SERVER, MYSQL_CDC_TO_KAFKA_TOPIC)
                )
                .uid("mysql_cdc_to_kafka_topic")
                .name("mysql_cdc_to_kafka_topic");

        cdcDbMainStreamMap.print("cdcDbMainStreamMap -> ");

        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMap = cdcDbDimStream.map(JSONObject::parseObject)
                .uid("db_dim_convert_json")
                .name("db_dim_convert_json")
                .setParallelism(1);

        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMapCleanColumn =cdcDbDimStreamMap.map(s->{
            s.remove("source");
            s.remove("transaction");
            JSONObject resJson = new JSONObject();
            if("d".equals(s.getString("op"))){
                resJson.put("before",s.getJSONObject("before"));
            }else {
                resJson.put("after",s.getJSONObject("after"));
            }
            resJson.put("op",s.getString("op"));
            return resJson;
        }).uid("clean_json_column_map")
                .name("clean_json_column_map");

        SingleOutputStreamOperator<JSONObject> tpDs=cdcDbDimStreamMapCleanColumn.map(new MapUpdateHbaseDimTableFunc(CDH_ZOOKEEPER_SERVER,CDH_HBASE_NAME_SPACE))
                .uid("map_create_hbase_dim_table")
                .name("map_create_hbase_dim_table");

        MapStateDescriptor<String, JSONObject> mapStageDesc =new MapStateDescriptor<>("map_state", String.class, JSONObject.class);
        BroadcastStream<JSONObject> broadcastDs = tpDs.broadcast(mapStageDesc);
        BroadcastConnectedStream<JSONObject, JSONObject> connectDs = cdcDbMainStreamMap.connect(broadcastDs);

        connectDs.process(new ProcessSpiltStreamToHBaseDimFunc(mapStageDesc));

        env.disableOperatorChaining();
        env.execute();
    }






    }



