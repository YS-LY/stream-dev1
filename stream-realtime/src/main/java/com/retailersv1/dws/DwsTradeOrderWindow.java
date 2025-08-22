package com.retailersv1.dws;
import com.stream.common.utils.Constant;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTradeOrderWindow {
    private static final String MYSQL_SINK_TABLE = "dws_trade_order_window";

    public static void main(String[] args) throws Exception {
        // 1. 初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(4);

        // 2. 创建Kafka Source表（处理时间字段pt）
        String kafkaSourceSql = "CREATE TABLE dwd_trade_order_detail (\n" +
                "  user_id STRING,\n" +
                "  ts BIGINT,\n" +
                "  pt AS PROCTIME()  -- 处理时间字段\n" +
                ")" + SqlUtil.getKafka(
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,
                "dws_trade_order_window"
        );
        tEnv.executeSql(kafkaSourceSql);

        // 3. 第一步：计算「当日首单」标记（仅一个OVER窗口）
        Table dayFirstOrderTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "  user_id,\n" +
                        "  DATE_FORMAT(pt, 'yyyyMMdd') AS cur_date,\n" +  // 处理时间日期
                        "  pt,\n" +
                        "  -- 按用户+日期分区，标记当日首单（day_rn=1）\n" +
                        "  ROW_NUMBER() OVER (\n" +
                        "    PARTITION BY user_id, DATE_FORMAT(pt, 'yyyyMMdd') \n" +
                        "    ORDER BY pt\n" +
                        ") AS day_rn \n" +
                        "FROM dwd_trade_order_detail"
        );
        tEnv.createTemporaryView("day_first_order_table", dayFirstOrderTable);

        // 4. 第二步：基于当日首单结果，计算「新用户首单」标记（第二个OVER窗口）
        Table allFirstOrderTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "  user_id,\n" +
                        "  cur_date,\n" +
                        "  pt,\n" +
                        "  day_rn,\n" +
                        "  -- 按用户分区，标记历史首单（all_rn=1）\n" +
                        "  ROW_NUMBER() OVER (\n" +
                        "    PARTITION BY user_id \n" +
                        "    ORDER BY pt\n" +
                        ") AS all_rn \n" +
                        "FROM day_first_order_table"  // 基于第一步结果计算，避免多窗口冲突
        );
        tEnv.createTemporaryView("all_first_order_table", allFirstOrderTable);

        // 5. 过滤有效记录，转换为指标
        Table indicatorTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "  user_id,\n" +
                        "  cur_date,\n" +
                        "  pt,\n" +
                        "  CASE WHEN day_rn = 1 THEN 1 ELSE 0 END AS order_uu_ct,\n" +  // 当日首单用户
                        "  CASE WHEN all_rn = 1 THEN 1 ELSE 0 END AS order_new_ct\n" +   // 新用户首单
                        "FROM all_first_order_table\n" +
                        "WHERE day_rn = 1  -- 仅保留当日首单记录"
        );
        tEnv.createTemporaryView("indicator_table", indicatorTable);

        // 6. 处理时间5秒滚动窗口聚合
        Table resultTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "  DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt,\n" +
                        "  DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt,\n" +
                        "  cur_date,\n" +
                        "  SUM(order_uu_ct) AS order_unique_user_count,\n" +
                        "  SUM(order_new_ct) AS order_new_user_count\n" +
                        "FROM TABLE(TUMBLE(TABLE indicator_table, DESCRIPTOR(pt), INTERVAL '5' SECOND))\n" +
                        "GROUP BY window_start, window_end, cur_date"
        );
//        resultTable.execute().print();
        tEnv.createTemporaryView("result_table", resultTable);

        // 7. 创建MySQL Sink表
        String mysqlSinkSql = "CREATE TABLE mysql_order_sink (\n" +
                "  stt STRING,\n" +
                "  edt STRING,\n" +
                "  cur_date STRING,\n" +
                "  order_unique_user_count BIGINT,\n" +
                "  order_new_user_count BIGINT,\n" +
                "  PRIMARY KEY (stt, edt, cur_date) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://cdh01:3306/gmall2025?useSSL=false&serverTimezone=Asia/Shanghai',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'table-name' = '" + MYSQL_SINK_TABLE + "',\n" +
                "  'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "  'sink.buffer-flush.max-rows' = '1',\n" +
                "  'sink.buffer-flush.interval' = '1000'\n" +
                ")";
        tEnv.executeSql(mysqlSinkSql);

        // 8. 写入MySQL
        resultTable.executeInsert("mysql_order_sink");

//        env.execute("DwsTradeOrderWindow");
    }
}
