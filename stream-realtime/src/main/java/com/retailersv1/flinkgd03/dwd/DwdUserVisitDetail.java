package com.retailersv1.flinkgd03.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.retailersv1.flinkgd03.dwd
 * @Author xiaoye
 * @Date 2025/8/19 10:02
 * @description: 用户访问明细事实表（最终无RowKey冲突版）
 */
public class DwdUserVisitDetail {
    private static final String ODS_TOPIC = ConfigUtils.getString("kafka.ods.user.visit.log.topic");
    private static final String DWD_TOPIC = ConfigUtils.getString("kafka.dwd.user.visit.detail");

    public static void main(String[] args) {
        try {
            // 1. 初始化Flink环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            EnvironmentSettingUtils.defaultParameter(env);
            env.setStateBackend(new MemoryStateBackend());
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            // 2. 注册ODS层Kafka表（语法无错，已验证）
            String odsFieldsSql = "CREATE TABLE ods_user_visit_log_kafka (" +
                    "    log_id BIGINT COMMENT '原始日志唯一ID'," +
                    "    visitor_id STRING COMMENT '访客唯一标识'," +
                    "    mid STRING COMMENT '设备ID'," +
                    "    visit_time BIGINT COMMENT '访问时间戳（毫秒）'," +
                    "    terminal_type STRING COMMENT '终端类型：pc/wireless'," +
                    "    visit_object_type STRING COMMENT '访问对象类型：shop/product'," +
                    "    visit_object_id BIGINT COMMENT '访问对象ID'," +
                    "    first_source STRING COMMENT '一级流量来源'," +
                    "    second_source STRING COMMENT '二级流量来源'," +
                    "    is_new_visitor TINYINT COMMENT '是否新访客：1-新/0-老'," +
                    "    page_view INT COMMENT '浏览量（PV）'," +
                    "    stay_time INT COMMENT '停留时间（秒）'," +
                    "    shop_id BIGINT COMMENT '店铺ID'," +
                    "    city_id INT COMMENT '城市ID（关联dim_city）'," +
                    "    gender STRING COMMENT '性别：male/female/unknown'," +
                    "    age STRING COMMENT '年龄段：18-25/26-30/31-40/40+'," +
                    "    taobao_score INT COMMENT '淘气值'," +
                    "    visit_time_ts AS TO_TIMESTAMP_LTZ(visit_time, 3)," +
                    "    WATERMARK FOR visit_time_ts AS visit_time_ts - INTERVAL '3' SECOND," +
                    "    proc_time AS proctime()" +
                    ")";
            String kafkaWithConfig = SqlUtil.getKafka(ODS_TOPIC, "dwd_user_visit_consumer");
            String fullOdsSql = odsFieldsSql + " " + kafkaWithConfig;
            System.out.println("=== ODS表完整SQL ===");
            System.out.println(fullOdsSql);
            tableEnv.executeSql(fullOdsSql);

            // 3. 注册HBase维度表dim_city（核心修复：删除显式主键，依赖第一个字段为RowKey）
            String dimCitySql = "CREATE TABLE dim_city (" +
                    "    city_id INT COMMENT '城市ID（默认作为HBase RowKey，必须是第一个字段）'," +
                    "    city_name STRING COMMENT '城市名称' " + // 最后字段无逗号
                    ") WITH (" +
                    "    'connector' = 'hbase-2.2'," +
                    "    'table-name' = 'default:dim_city'," + // HBase表名（命名空间:表名）
                    "    'zookeeper.quorum' = 'cdh01:2181,cdh02:2181,cdh03:2181'," + // ZK地址
                    "    'lookup.async' = 'true'," + // 异步查询（提升性能）
                    "    'lookup.cache.max-rows' = '500'," + // 缓存行数
                    "    'lookup.cache.ttl' = '1 hour'" + // 缓存过期时间
                    ")";
            System.out.println("\n=== dim_city表完整SQL ===");
            System.out.println(dimCitySql);
            tableEnv.executeSql(dimCitySql); // 无RowKey重复错误

            // 4. 注册DWD层Upsert-Kafka表
            String dwdFieldsSql = "CREATE TABLE " + DWD_TOPIC + " (" +
                    "    log_id BIGINT COMMENT '日志ID（DWD唯一标识）'," +
                    "    visitor_id STRING COMMENT '访客ID'," +
                    "    mid STRING COMMENT '设备ID'," +
                    "    visit_time_ts TIMESTAMP(3) COMMENT '访问时间'," +
                    "    terminal_type STRING COMMENT '终端类型'," +
                    "    visit_object_type STRING COMMENT '访问对象类型'," +
                    "    visit_object_id BIGINT COMMENT '访问对象ID'," +
                    "    first_source STRING COMMENT '一级来源'," +
                    "    second_source STRING COMMENT '二级来源'," +
                    "    is_new_visitor TINYINT COMMENT '是否新访客'," +
                    "    page_view INT COMMENT 'PV'," +
                    "    stay_time INT COMMENT '停留时间'," +
                    "    shop_id BIGINT COMMENT '店铺ID'," +
                    "    city_id INT COMMENT '城市ID'," +
                    "    city_name STRING COMMENT '城市名称（HBase关联）'," +
                    "    gender STRING COMMENT '性别'," +
                    "    age STRING COMMENT '年龄段'," +
                    "    taobao_score INT COMMENT '淘气值'," +
                    "    PRIMARY KEY (log_id) NOT ENFORCED" + // DWD表主键（Upsert必需）
                    ")";
            String dwdWithConfig = SqlUtil.getUpsertKafkaDDL(DWD_TOPIC);
            String fullDwdSql = dwdFieldsSql + " " + dwdWithConfig;
            System.out.println("\n=== DWD表完整SQL ===");
            System.out.println(fullDwdSql);
            tableEnv.executeSql(fullDwdSql);

            // 5. 关联计算：ODS + HBase维表
            String querySql = "SELECT " +
                    "    o.log_id, o.visitor_id, o.mid, o.visit_time_ts, o.terminal_type," +
                    "    o.visit_object_type, o.visit_object_id, o.first_source, o.second_source," +
                    "    o.is_new_visitor, o.page_view, o.stay_time, o.shop_id, o.city_id," +
                    "    c.city_name, o.gender, o.age, o.taobao_score " +
                    "FROM ods_user_visit_log_kafka o " +
                    "LEFT JOIN dim_city FOR SYSTEM_TIME AS OF o.proc_time AS c " +
                    "ON o.city_id = c.city_id"; // 关联条件（字段类型一致：INT）

            // 6. 结果写入DWD Kafka
            Table resultTable = tableEnv.sqlQuery(querySql);
            resultTable.executeInsert(DWD_TOPIC);

            // 7. 启动任务
            env.execute("DwdUserVisitDetail_Final_Job");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}