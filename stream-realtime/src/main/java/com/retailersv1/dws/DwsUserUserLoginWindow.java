package com.retailersv1.dws;


import com.stream.common.utils.Constant;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//12.4用户域用户登录各窗口汇总表
/**
 * 用户域用户登录窗口表
 * 功能：从Kafka读取页面日志，统计窗口内独立用户数和回流用户数，写入Doris
 */
public class DwsUserUserLoginWindow {
    private static final String DWD_TRAFFIC_PAGE_TOPIC = Constant.TOPIC_DWD_TRAFFIC_PAGE;
    private static final String DWS_USER_LOGIN_SINK_IDENTIFIER =
            Constant.DORIS_DATABASE + ".dws_user_user_login_window";

    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 设置状态后端（生产环境建议使用RocksDBStateBackend）
        env.setStateBackend(new MemoryStateBackend());

        // 2. 从Kafka读取页面日志数据并设置水位线
        tEnv.executeSql("CREATE TABLE ods_page_log (\n" +
                "  common MAP<STRING, STRING>,\n" +  // 包含用户ID等公共信息
                "  page MAP<STRING, STRING>,\n" +   // 包含页面跳转信息
                "  ts BIGINT,\n" +                  // 时间戳
                "  et AS to_timestamp_ltz(ts, 3),\n" +  // 事件时间（毫秒级）
                "  watermark for et as et - interval '5' second \n" +  // 允许5秒延迟
                ")" + SqlUtil.getKafka(
                DWD_TRAFFIC_PAGE_TOPIC, "dws_user_user_login_source")  // 复用Kafka连接工具类
        );

        // 3. 过滤登录行为数据（自动登录或手动登录）
        Table loginTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "common['uid'] uid,\n" +  // 用户唯一标识
                        "ts,\n" +
                        "et,\n" +
                        "date_format(et, 'yyyyMMdd') cur_date \n" +  // 当天日期（yyyyMMdd）
                        "FROM ods_page_log \n" +
                        "WHERE common['uid'] IS NOT NULL \n" +  // 必须有用户ID
                        "AND (page['last_page_id'] IS NULL OR page['last_page_id'] = 'login')"  // 登录行为判断
        );
//        loginTable.execute().print();
        tEnv.createTemporaryView("login_table", loginTable);

        // 4. 标记用户当天首次登录（计算独立用户数基础）
        Table firstLoginPerDayTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "uid,\n" +
                        "ts,\n" +
                        "et,\n" +
                        "cur_date, \n" +
                        "row_number() OVER (\n" +
                        "  PARTITION BY uid, cur_date \n" +  // 按用户+当天分区
                        "  ORDER BY et ASC \n" +  // 按时间升序，取第一条为当天首次登录
                        ") rn \n" +
                        "FROM login_table"
        );
        firstLoginPerDayTable.execute().print();
        tEnv.createTemporaryView("first_login_per_day", firstLoginPerDayTable);

        // 5. 过滤出当天首次登录记录，并计算回流用户标识
        Table loginWithBackFlagTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "uid,\n" +
                        "ts,\n" +
                        "et,\n" +
                        "cur_date, \n" +
                "lag(cur_date, 1) OVER (\n" +
                        "  PARTITION BY uid \n" +
                                "  ORDER BY cur_date ASC \n" +
                                ") last_login_date \n" +
                                "FROM first_login_per_day \n" +
                                "WHERE rn = 1"  // 仅保留当天首次登录记录
                );
//        loginWithBackFlagTable.execute().print();
        tEnv.createTemporaryView("login_with_back_flag", loginWithBackFlagTable);

        // 6. 计算回流用户数（上次登录距今天数>7）
        Table loginMetricTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "uid,\n" +
                        "ts,\n" +
                        "et,\n" +
                        "cur_date, \n" +
                        "1L AS uu_ct,  -- 当天首次登录，独立用户数+1\n" +
                "CASE \n" +
                        "  WHEN last_login_date IS NOT NULL \n" +
                        "   AND datediff(to_date(cur_date, 'yyyyMMdd'), to_date(last_login_date, 'yyyyMMdd')) > 7 \n" +
                        "  THEN 1L \n" +
                        "  ELSE 0L \n" +
                        "END AS back_ct \n" +
                        "FROM login_with_back_flag"
        );
        tEnv.createTemporaryView("login_metric_table", loginMetricTable);

        // 7. 开窗聚合（5秒窗口）
        Table resultTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +  // 窗口起始时间
                        "date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +    // 窗口结束时间
                        "date_format(window_start, 'yyyyMMdd') cur_date,\n" +       // 窗口日期
                        "SUM(uu_ct) AS uu_ct,\n" +  // 窗口内独立用户总数
                        "SUM(back_ct) AS back_ct \n" +  // 窗口内回流用户总数
                        "FROM table(tumble(table login_metric_table, descriptor(et), interval '5' second )) \n" +  // 5秒滚动窗口
                        "GROUP BY window_start, window_end"
        );
        // resultTable.execute().print();  // 调试用

        // 8. 创建Doris Sink表
        String createSinkSql = "CREATE TABLE dws_user_user_login_window (\n" +
                "stt STRING COMMENT '窗口起始时间',\n" +
                "edt STRING COMMENT '窗口结束时间',\n" +
                "cur_date STRING COMMENT '当天日期',\n" +
                "uu_ct BIGINT COMMENT '独立用户数',\n" +
                "back_ct BIGINT COMMENT '回流用户数'\n" +
                ")" + "WITH (\n" +
                " 'connector' = 'doris',\n" +
                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "',\n" +
                " 'table.identifier' = '" + DWS_USER_LOGIN_SINK_IDENTIFIER + "',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456', \n" +
                " 'sink.properties.format' = 'json', \n" +
                " 'sink.buffer-count' = '4', \n" +
                " 'sink.buffer-size' = '4086',\n" +
                " 'sink.enable-2pc' = 'true', \n" +  // 两阶段提交确保精确一次语义
                " 'sink.properties.read_json_by_line' = 'true' \n" +
                ")";
        tEnv.executeSql(createSinkSql);

        // 9. 将结果写入Doris
        resultTable.executeInsert("dws_user_user_login_window");
    }
}