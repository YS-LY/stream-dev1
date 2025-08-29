package com.retailersv1.flinkgd03.ads;

import com.stream.common.utils.Constant;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class AdsTrafficOverview {
    // 依赖的DWS层Kafka主题（假设）
    private static final String DWS_TRAFFIC_TOPIC = "FlinkGd03_dws_traffic_overview";
    // 目标ADS表名
    private static final String MYSQL_SINK_TABLE = "ads_traffic_overview";

    public static void main(String[] args) throws Exception {
        // 1. 初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(4);

        // 2. 从Kafka读取DWS层流量数据（处理时间对齐）
        tEnv.executeSql("CREATE TABLE dws_traffic_overview (\n" +
                "  terminal_type STRING,\n" +
                "  shop_id BIGINT,\n" +
                "  shop_visitor_count BIGINT,\n" +
                "  shop_page_view BIGINT,\n" +
                "  new_visitor_count BIGINT,\n" +
                "  old_visitor_count BIGINT,\n" +
                "  product_visitor_count BIGINT,\n" +
                "  product_page_view BIGINT,\n" +
                "  add_collection_count BIGINT,\n" +
                "  pay_buyer_count BIGINT,\n" +
                "  pay_amount DECIMAL(18,2),\n" +
                "  pt AS PROCTIME()  -- 处理时间字段，与DWS层对齐\n" +
                ")" + SqlUtil.getKafka(
                DWS_TRAFFIC_TOPIC, "ads_traffic_overview")
        );

        // 3. 聚合计算（按统计时间、终端类型、店铺ID分组）
        Table resultTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "  TO_TIMESTAMP(date_format(window_start, 'yyyy-MM-dd HH:mm:ss')) AS stat_time,  -- 统计时间（窗口起始）\n" +
                        "  terminal_type,  -- 终端类型（overall/pc/wireless）\n" +
                        "  shop_id,  -- 店铺ID\n" +
                        "  SUM(shop_visitor_count) AS shop_visitor_count,  -- 累计店铺访客数\n" +
                        "  SUM(shop_page_view) AS shop_page_view,  -- 累计店铺PV\n" +
                        "  SUM(new_visitor_count) AS new_visitor_count,  -- 累计新访客数\n" +
                        "  SUM(old_visitor_count) AS old_visitor_count,  -- 累计老访客数\n" +
                        "  SUM(product_visitor_count) AS product_visitor_count,  -- 累计商品访客数\n" +
                        "  SUM(product_page_view) AS product_page_view,  -- 累计商品PV\n" +
                        "  SUM(add_collection_count) AS add_collection_count,  -- 累计加购收藏人数\n" +
                        "  SUM(pay_buyer_count) AS pay_buyer_count,  -- 累计支付买家数\n" +
                        "  SUM(pay_amount) AS pay_amount,  -- 累计支付金额\n" +
                        "  -- 计算支付转化率（避免除数为0）\n" +
                        "  CASE \n" +
                        "    WHEN SUM(shop_visitor_count) = 0 THEN 0.0000 \n" +
                        "    ELSE ROUND(SUM(pay_buyer_count) / SUM(shop_visitor_count), 4) \n" +
                        "  END AS pay_conversion_rate, \n" +
                        "  CURRENT_TIMESTAMP() AS create_time,  -- 在SQL逻辑中生成创建时间\n" +
                        "  CURRENT_TIMESTAMP() AS update_time   -- 在SQL逻辑中生成更新时间（后续可通过UPSERT逻辑更新）\n" +
                        "FROM table(tumble(table dws_traffic_overview, descriptor(pt), interval '5' MINUTE))  -- 5分钟窗口（与DWS对齐）\n" +
                        "GROUP BY window_start, terminal_type, shop_id"
        );
        tEnv.createTemporaryView("result_table", resultTable);

        // 4. 创建MySQL Sink表（映射ads_traffic_overview结构，移除DEFAULT）
        String mysqlSinkSql = "CREATE TABLE mysql_traffic_overview_sink (\n" +
                "  stat_time DATETIME NOT NULL,\n" +
                "  terminal_type VARCHAR(20) NOT NULL,\n" +
                "  shop_id BIGINT NOT NULL,\n" +
                "  shop_visitor_count BIGINT NOT NULL,\n" +  // 移除DEFAULT 0
                "  shop_page_view BIGINT NOT NULL,\n" +       // 移除DEFAULT 0
                "  new_visitor_count BIGINT NOT NULL,\n" +    // 移除DEFAULT 0
                "  old_visitor_count BIGINT NOT NULL,\n" +    // 移除DEFAULT 0
                "  product_visitor_count BIGINT NOT NULL,\n" + // 移除DEFAULT 0
                "  product_page_view BIGINT NOT NULL,\n" +     // 移除DEFAULT 0
                "  add_collection_count BIGINT NOT NULL,\n" +  // 移除DEFAULT 0
                "  pay_buyer_count BIGINT NOT NULL,\n" +       // 移除DEFAULT 0
                "  pay_amount DECIMAL(18,2) NOT NULL,\n" +     // 移除DEFAULT 0.00
                "  pay_conversion_rate DECIMAL(8,4) NOT NULL,\n" +  // 移除DEFAULT 0.0000
                "  create_time DATETIME NOT NULL,\n" +         // 移除DEFAULT CURRENT_TIMESTAMP
                "  update_time DATETIME NOT NULL,\n" +         // 移除DEFAULT CURRENT_TIMESTAMP ON UPDATE...
                "  PRIMARY KEY (stat_time, terminal_type, shop_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://cdh01:3306/gmall2025?useSSL=false&serverTimezone=Asia/Shanghai',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'table-name' = '" + MYSQL_SINK_TABLE + "',\n" +
                "  'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "  'sink.buffer-flush.max-rows' = '100',\n" +  // 批量写入优化
                "  'sink.buffer-flush.interval' = '5000',\n" +
                "  'sink.upsert-mode' = 'true'  -- 支持UPSERT（更新已有记录）\n" +  // 新增UPSERT配置，用于更新update_time
                ")"
                ;
        tEnv.executeSql(mysqlSinkSql);

        // 5. 写入ADS层MySQL表
        resultTable.executeInsert("mysql_traffic_overview_sink");

        env.execute("AdsTrafficOverview");
    }
}