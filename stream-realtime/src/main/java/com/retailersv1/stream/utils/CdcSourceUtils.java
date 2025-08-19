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
    public static MySqlSource<String> getMySQLCdcSource(String database, String table, String username, String pwd, StartupOptions model,String serverId){

        // 创建Debezium的配置属性对象（Debezium是CDC的核心组件）
        Properties debeziumProperties = new Properties();

        // 设置数据库连接的字符集为UTF-8，避免中文乱码
        debeziumProperties.setProperty("database.connectionCharset", "UTF-8");

        // 设置Decimal类型的处理模式为字符串，避免精度丢失（如金额字段）
        debeziumProperties.setProperty("decimal.handling.mode","string");

        // 设置时间精度模式为"connect"，统一时间类型的序列化格式（遵循Debezium的时间标准）
        debeziumProperties.setProperty("time.precision.mode","connect");

        // 设置快照模式为"schema_only"，表示初始化时只获取表结构，不读取历史数据
        debeziumProperties.setProperty("snapshot.mode", "schema_only");

        // 设置是否包含表结构变更事件，false表示不捕获DDL（如建表、ALTER等操作）
        debeziumProperties.setProperty("include.schema.changes", "false");

        // 设置数据库连接的时区为Asia/Shanghai（上海时区），确保时间字段转换正确
        debeziumProperties.setProperty("database.connectionTimeZone", "Asia/Shanghai");
        return  MySqlSource.<String>builder()
                .hostname(ConfigUtils.getString("mysql.host"))
                .port(ConfigUtils.getInt("mysql.port"))
                .databaseList(database)
                .tableList(table)
                .username(username)
                .password(pwd)
                // 配置Debezium的server-id范围（
                .serverId(serverId)
//                .connectionTimeZone(ConfigUtils.getString("mysql.timezone"))、
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(model)
                .includeSchemaChanges(true)
                .debeziumProperties(debeziumProperties)
                .build();
    }
}
