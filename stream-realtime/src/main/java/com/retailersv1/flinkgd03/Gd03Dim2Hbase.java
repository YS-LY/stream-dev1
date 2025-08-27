package com.retailersv1.flinkgd03;


import com.alibaba.fastjson.JSONObject;
import com.retailersv1.stream.utils.CdcSourceUtils;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.HbaseUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.HashMap;
import java.util.Map;

/**
 * 将DIM层数据同步到HBase（解决server-id冲突问题）
 */
public class Gd03Dim2Hbase {

    // 为每个表分配唯一的server-id（确保全局唯一，不与MySQL主节点和其他同步工具冲突）
    private static final Map<String, Integer> TABLE_SERVER_ID_MAP = new HashMap<String, Integer>() {{
        put("dim_product", 1001);       // 产品表专用server-id
        put("dim_page", 1002);          // 页面表专用server-id
        put("dim_traffic_source", 1003);// 流量来源表专用server-id
        put("dim_city", 1004);          // 城市表专用server-id
        put("dim_taoqi_level", 1005);   // 淘气等级表专用server-id
    }};

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 初始化HBase表结构
        HbaseUtils hbaseUtils = new HbaseUtils(ConfigUtils.getString("zookeeper.server.host.list"));
        hbaseUtils.initGd03DimTables();

        String database = ConfigUtils.getString("mysql.databases.conf");

        // 同步各个DIM表数据到HBase（传入每个表唯一的server-id）
        syncDimTable(env, database, "product", "dim_product", "product_id");
        syncDimTable(env, database, "page", "dim_page", "page_id");
        syncDimTable(env, database, "traffic_source", "dim_traffic_source", "source_id");
        syncDimTable(env, database, "city", "dim_city", "city_id");
        syncDimTable(env, database, "taoqi_level", "dim_taoqi_level", "level_id");

        env.execute("Gd03 DIM to HBase");
    }

    /**
     * 同步单个DIM表到HBase（为每个表设置唯一server-id）
     */
    private static void syncDimTable(StreamExecutionEnvironment env, String database,
                                     String sourceTableName, String dimTableName, String rowKeyField) throws Exception {
        // 获取当前表对应的唯一server-id
        Integer serverId = TABLE_SERVER_ID_MAP.get(dimTableName);
        if (serverId == null) {
            throw new RuntimeException("未为表" + dimTableName + "配置唯一的server-id，请检查TABLE_SERVER_ID_MAP");
        }

        // 创建Debezium配置，设置唯一server-id
        Map<String, String> debeziumProps = new HashMap<>();
        debeziumProps.put("database.server.id", serverId.toString());
        // 设置独立的offset存储路径，避免不同表共享offset导致冲突
        debeziumProps.put("offset.storage.file.filename",
                "/tmp/flink-cdc-offset-" + dimTableName + "-" + serverId + ".dat");
        // 可选：设置server-uuid增强唯一性
        debeziumProps.put("database.server.uuid", "flink-cdc-" + dimTableName + "-" + serverId);

        // 创建带唯一server-id配置的CDC源
        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname(ConfigUtils.getString("mysql.host"))  // 补充MySQL主机配置
                .port(Integer.parseInt(ConfigUtils.getString("mysql.port")))  // 补充端口配置
                .username(ConfigUtils.getString("mysql.user"))
                .password(ConfigUtils.getString("mysql.pwd"))
                .databaseList(database)
                .tableList(database + "." + sourceTableName)
                .startupOptions(StartupOptions.earliest())
                .deserializer(new com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> stream = env.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                sourceTableName + "_source");

        stream.map(JSONObject::parseObject)
                .addSink(new Dim2HBaseSinkFunction(dimTableName, rowKeyField))
                .name("sink_" + dimTableName)
                .uid("sink_" + dimTableName);
    }

    /**
     * DIM数据写入HBase的Sink函数
     */
    public static class Dim2HBaseSinkFunction extends RichSinkFunction<JSONObject> {
        private final String dimTableName;
        private final String rowKeyField;
        private HbaseUtils hbaseUtils;
        private org.apache.hadoop.hbase.client.Connection connection;

        public Dim2HBaseSinkFunction(String dimTableName, String rowKeyField) {
            this.dimTableName = dimTableName;
            this.rowKeyField = rowKeyField;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            hbaseUtils = new HbaseUtils(ConfigUtils.getString("zookeeper.server.host.list"));
            connection = hbaseUtils.getConnection();
        }

        @Override
        public void invoke(JSONObject jsonObject, Context context) throws Exception {
            String op = jsonObject.getString("op");

            // 只处理创建、更新、读取操作，忽略删除操作
            if (!"d".equals(op)) {
                JSONObject data = jsonObject.getJSONObject("after");
                if (data == null) {
                    data = jsonObject.getJSONObject("before");
                }

                if (data != null) {
                    String rowKey = data.getString(rowKeyField);
                    if (rowKey != null) {
                        Table table = connection.getTable(TableName.valueOf("FlinkGd03:" + dimTableName));
                        Put put = new Put(Bytes.toBytes(rowKey));

                        // 将所有字段添加到info列族中
                        for (String key : data.keySet()) {
                            Object value = data.get(key);
                            if (value != null) {
                                put.addColumn(Bytes.toBytes("info"),
                                        Bytes.toBytes(key),
                                        Bytes.toBytes(value.toString()));
                            }
                        }

                        table.put(put);
                        table.close();
                        System.out.println("成功写入数据到HBase表: " + dimTableName + ", RowKey: " + rowKey);
                    }
                }
            }
        }

        @Override
        public void close() throws Exception {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }
}
