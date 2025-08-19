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


}