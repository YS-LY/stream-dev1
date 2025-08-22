package com.retailersv1.dws;




import com.stream.common.utils.Constant;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// 交易域购物车添加独立用户窗口汇总表（基于处理时间）
public class DwsTradeCartAddUuWindow {
    // Kafka数据源主题
    private static final String DWD_TRADE_CART_ADD_TOPIC = Constant.TOPIC_DWD_TRADE_CART_ADD;
    // MySQL目标表标识
    private static final String MYSQL_SINK_TABLE = "dws_trade_cart_add_uu_window";

    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2. 从Kafka读取购物车添加数据（使用处理时间）
        tEnv.executeSql("CREATE TABLE dwd_cart_info (\n" +
                "  user_id STRING,\n" +  // 用户ID
                "  ts BIGINT,\n" +  // 原始时间戳（仅作为参考）
                "  pt AS PROCTIME()  -- 定义处理时间字段\n" +
                ")" + SqlUtil.getKafka(
                DWD_TRADE_CART_ADD_TOPIC, "dws_trade_cart_add_uu_window")
        );

        // 3. 提取处理时间日期，标记用户当日首次添加行为
        Table firstAddTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "user_id,\n" +
                        "date_format(pt, 'yyyyMMdd') AS cur_date,\n" +  // 基于处理时间的日期
                        "pt  -- 保留处理时间用于窗口计算\n" +
                        "FROM (\n" +
                        "  SELECT \n" +
                        "  user_id,\n" +
                        "  pt,\n" +
                        "  -- 按用户和日期分组，标记首次添加行为\n" +
                        "  row_number() OVER (\n" +
                        "    PARTITION BY user_id, date_format(pt, 'yyyyMMdd') \n" +
                        "    ORDER BY pt\n" +
                        "  ) AS rn\n" +
                        "  FROM dwd_cart_info\n" +
                        ") t\n" +
                        "WHERE rn = 1"  // 只保留每个用户当日首次添加记录
        );
        tEnv.createTemporaryView("first_add_table", firstAddTable);

        // 4. 基于处理时间做5秒滚动窗口统计独立用户数
        Table resultTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "date_format(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt,\n" +  // 窗口起始时间
                        "date_format(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt,\n" +  // 窗口结束时间
                        "cur_date,\n" +  // 统计日期（基于处理时间）
                        "count(user_id) AS cart_add_uu_ct  -- 独立用户数\n" +
                        "FROM table(tumble(table first_add_table, descriptor(pt), interval '5' SECOND)) \n" +  // 5秒滚动窗口
                        "GROUP BY window_start, window_end, cur_date"
        );
//        resultTable.execute().print();
        tEnv.createTemporaryView("result_table", resultTable);

        // 5. 创建MySQL Sink表
        String mysqlSinkSql = "CREATE TABLE mysql_cart_add_uu_sink (\n" +
                "  stt STRING,\n" +
                "  edt STRING,\n" +
                "  cur_date STRING,\n" +
                "  cart_add_uu_ct BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://cdh01:3306/gmall2025?useSSL=false&serverTimezone=Asia/Shanghai',\n" +  // 替换为实际MySQL地址
                "  'username' = 'root',\n" +  // 实际MySQL用户名
                "  'password' = '123456',\n" +  // 实际MySQL密码
                "  'table-name' = '" + MYSQL_SINK_TABLE + "',\n" +  // MySQL目标表名
                "  'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "  'sink.buffer-flush.max-rows' = '1',\n" +  // 测试用：1条即刷写
                "  'sink.buffer-flush.interval' = '1000'\n" +
                ")"
                ;
        tEnv.executeSql(mysqlSinkSql);

        // 6. 将结果写入MySQL
        resultTable.executeInsert("mysql_cart_add_uu_sink");

        // 启动任务
//        env.execute("DwsTradeCartAddUuWindow");
    }
}