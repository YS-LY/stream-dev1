package com.retailersv1.dwd;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.domain.TableProcessDim;
import com.retailersv1.func.TableProcessDwd;
import com.retailersv1.stream.utils.CdcSourceUtils;
import com.stream.common.utils.*;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DwdBaseDb {
    private static final String kafka_bootstrap_server = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // 创建kafka数据源
        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_bootstrap_server,
                        ODS_KAFKA_TOPIC,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ), WatermarkStrategy.noWatermarks(), "read_kafka_realtime_db"
        );
//        kafkaSourceDs.print() ;

        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaSourceDs.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObj = JSONObject.parseObject(s);
                    collector.collect(jsonObj);
                } catch (Exception e) {
                    System.out.println("非标准JSON");
                }
            }
        });
        MySqlSource<String> mySQLCdcDwdConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "realtime_v1_config.table_process_dwd",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial(),
                "10000-10050"
        );
        DataStreamSource<String> cdcDbDwdStream = env.fromSource(mySQLCdcDwdConfSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_dwd_source");

        SingleOutputStreamOperator<TableProcessDwd> tpDs = cdcDbDwdStream.map(new MapFunction<String, TableProcessDwd>() {
            @Override
            public TableProcessDwd map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                String op = jsonObj.getString("op");
                TableProcessDwd tp = null;
                if ("d".equals(op)){
                    tp = jsonObj.getObject("before", TableProcessDwd.class);
                }else {
                    tp = jsonObj.getObject("after", TableProcessDwd.class);
                }
                tp.setOp( op);
                return tp;
            }
        });
        tpDs.print();

        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDs = tpDs.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDs = jsonObjDs.connect(broadcastDs);

        connectDs.process(
                new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
                    private Map<String, TableProcessDwd> configMap = new HashMap<>();
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Connection connection = JdbcUtils.getMySQLConnection(
                                ConfigUtils.getString("mysql.url"),
                                ConfigUtils.getString("mysql.user"),
                                ConfigUtils.getString("mysql.pwd"));
                        String querySQL = "select * from realtime_v1_config.table_process_dim";
                        List<TableProcessDim> tableProcessDims = JdbcUtils.queryList(connection, querySQL, TableProcessDim.class, true);
                    }


                    @Override
                    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {

                    }

                    @Override
                    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {

                    }
                }
        );

        env.execute();

    }
}