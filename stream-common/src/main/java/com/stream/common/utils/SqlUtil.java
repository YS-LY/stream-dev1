package com.stream.common.utils;



public class SqlUtil {
    private static final String HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    private static final String KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");



    public static String getKafka(String topic, String groupId){
        return  "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+topic+"',\n" +
                "  'properties.bootstrap.servers' = '"+KAFKA_SERVER+"',\n" +
                "  'properties.group.id' = '"+groupId+"',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    public static String getHbaseDDL(String tableName){
        return  " WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '"+HBASE_NAME_SPACE+":"+tableName+"',\n" +
                " 'zookeeper.quorum' = '"+ZOOKEEPER_SERVER+"',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache.max-rows' = '500',\n" +
                " 'lookup.cache.ttl' = '1 hour'\n" +
                ")";
    }

    public static String getUpsertKafkaDDL(String topic){
        return  " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+topic+"',\n" +
                "  'properties.bootstrap.servers' = '"+KAFKA_SERVER+"',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }

    /**
     * 生成Kafka源表的DDL配置（用于Flink SQL创建Kafka源表）
     * @param groupId 消费者组ID
     * @param topic Kafka主题名
     * @return 拼接好的Kafka源表DDL的WITH部分
     */
    public static String getKafkaDDLSource(String groupId, String topic) {
        // 拼接Kafka源表的WITH配置（Flink SQL语法）
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +  // 连接器类型：kafka
                "  'topic' = '" + topic + "',\n" +  // 订阅的Kafka主题
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BOOTSTRAP_SERVERS + "',\n" +  // Kafka集群地址（从常量类获取）
                "  'properties.group.id' = '" + groupId + "',\n" +  // 消费者组ID（方法参数传入）
                "  'format' = 'json',\n" +  // 数据格式：json（根据实际数据格式调整，如csv、avro等）
                "  'scan.startup.mode' = 'latest-offset',\n" +  // 消费起始位置：最新偏移量（可根据需求改为earliest-offset）
                "  'json.fail-on-missing-field' = 'false',\n" +  // 解析json时忽略缺失字段（避免解析失败）
                "  'json.ignore-parse-errors' = 'true'\n" +  // 忽略解析错误（容错处理）
                ")";
    }


}