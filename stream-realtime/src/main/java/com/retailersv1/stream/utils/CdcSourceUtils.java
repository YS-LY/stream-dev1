package com.retailersv1.stream.utils;

import com.stream.common.utils.ConfigUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import java.util.Properties;

/**
 * @Package com.stream.utils
 * @Author xiaoye
 * @Date 2025/8/15 14:21
 * @description:
 */
public class CdcSourceUtils {

    /**
     * 获取MySQL CDC数据源（基于Debezium）
     * @param database 要监控的数据库名
     * @param table 要监控的表名（格式：数据库名.表名）
     * @param username 数据库用户名
     * @param pwd 数据库密码
     * @param model 启动选项（如初始快照、从最新位置开始等）
     * @return 配置好的MySQL CDC数据源对象
     */
    public static MySqlSource<String> getMySQLCdcSource(String database,String table,String username,String pwd,String serverId,StartupOptions model){
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("database.connectionCharset", "UTF-8");
        debeziumProperties.setProperty("decimal.handling.mode","string");
        debeziumProperties.setProperty("time.precision.mode","connect");
        debeziumProperties.setProperty("snapshot.mode", "schema_only");
        debeziumProperties.setProperty("include.schema.changes", "false");
        debeziumProperties.setProperty("database.connectionTimeZone", "Asia/Shanghai");
        return  MySqlSource.<String>builder()
                .hostname(ConfigUtils.getString("mysql.host"))
                .port(ConfigUtils.getInt("mysql.port"))
                .databaseList(database)
                .tableList(table)
                .username(username)
                .password(pwd)
                .serverId(serverId)
//                .connectionTimeZone(ConfigUtils.getString("mysql.timezone"))、
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(model)
                .includeSchemaChanges(true)
                .debeziumProperties(debeziumProperties)
                .build();
    }
}
