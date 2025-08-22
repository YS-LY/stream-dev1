package com.retailersv1.dws;


import com.stream.common.utils.Constant;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTradePaymentSucWindow {
    private static final String DWD_TRADE_PAYMENT_TOPIC = Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS;
    private static final String MYSQL_SINK_TABLE = "dws_trade_payment_suc_window";

    public static void main(String[] args) throws Exception {
        // 1. 初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(4);

        // 2. 从Kafka读取支付成功数据（使用处理时间）
        tEnv.executeSql("CREATE TABLE dwd_trade_payment_success (\n" +
                "  user_id STRING,\n" +
                "  ts BIGINT,\n" +
                "  pt AS PROCTIME()  -- 处理时间字段\n" +
                ")" + SqlUtil.getKafka(
                DWD_TRADE_PAYMENT_TOPIC, "dws_trade_payment_suc_window")
        );

        // 3. 第一步：计算用户当日首次支付标记（拆分为单独的Over窗口）
        Table dayFirstPayTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "user_id,\n" +
                        "date_format(pt, 'yyyyMMdd') AS cur_date,\n" +
                        "pt,\n" +
                        "ROW_NUMBER() OVER (\n" +
                        "  PARTITION BY user_id, date_format(pt, 'yyyyMMdd') \n" +
                        "  ORDER BY pt\n" +
                        ") AS day_rn  -- 仅保留当日首次支付的窗口\n" +
                        "FROM dwd_trade_payment_success"
        );
        tEnv.createTemporaryView("day_first_pay_table", dayFirstPayTable);

        // 4. 第二步：基于第一步结果，计算用户历史首次支付标记（第二个Over窗口）
        Table allFirstPayTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "user_id,\n" +
                        "cur_date,\n" +
                        "pt,\n" +
                        "day_rn,\n" +
                        "ROW_NUMBER() OVER (\n" +
                        "  PARTITION BY user_id \n" +
                        "  ORDER BY pt\n" +
                        ") AS all_rn  -- 历史首次支付的窗口\n" +
                        "FROM day_first_pay_table"
        );
        tEnv.createTemporaryView("all_first_pay_table", allFirstPayTable);

        // 5. 过滤有效记录并转换指标（与原逻辑一致）
        Table indicatorTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "user_id,\n" +
                        "cur_date,\n" +
                        "pt,\n" +
                        "CASE WHEN day_rn = 1 THEN 1 ELSE 0 END AS pay_uu_ct,\n" +  // 当日首次支付
                        "CASE WHEN all_rn = 1 THEN 1 ELSE 0 END AS pay_new_ct\n" +  // 历史首次支付（新用户）
                        "FROM all_first_pay_table\n"
//                        "WHERE day_rn = 1  -- 只保留当日首次支付记录"
        );
        tEnv.createTemporaryView("indicator_table", indicatorTable);

        // 6. 基于处理时间做5秒滚动窗口聚合
        Table resultTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "date_format(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt,\n" +
                        "date_format(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt,\n" +
                        "cur_date,\n" +
                        "SUM(pay_uu_ct) AS payment_suc_unique_user_count,\n" +
                        "SUM(pay_new_ct) AS payment_suc_new_user_count\n" +
                        "FROM table(tumble(table indicator_table, descriptor(pt), interval '5' SECOND))\n" +
                        "GROUP BY window_start, window_end, cur_date"
        );
//        resultTable.execute().print();
        tEnv.createTemporaryView("result_table", resultTable);

        // 7. 创建MySQL Sink表（保持不变）
        String mysqlSinkSql = "CREATE TABLE mysql_payment_suc_sink (\n" +
                "  stt STRING,\n" +
                "  edt STRING,\n" +
                "  cur_date STRING,\n" +
                "  payment_suc_unique_user_count BIGINT,\n" +
                "  payment_suc_new_user_count BIGINT,\n" +
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
                ")"
                ;
        tEnv.executeSql(mysqlSinkSql);

        // 8. 写入MySQL
        resultTable.executeInsert("mysql_payment_suc_sink");

//        env.execute("DwsTradePaymentSucWindow");
    }
}
