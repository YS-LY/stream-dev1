package com.retailersv1.dws;


import com.stream.common.utils.Constant;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 流量域版本渠道地区新老访客页面浏览窗口表（基于处理时间）
 * 功能：从Kafka读取页面浏览数据，按版本、渠道、地区、新老访客分组，统计窗口内UV、SV、PV及累计时长，写入Doris
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
    private static final String DWD_TRAFFIC_PAGE_TOPIC = Constant.TOPIC_DWD_TRAFFIC_PAGE;
    private static final String DWS_TRAFFIC_VC_CH_AR_IS_NEW_SINK_IDENTIFIER =
            Constant.DORIS_DATABASE + ".dws_traffic_vc_ch_ar_is_new_page_view_window";

    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 设置状态后端
        env.setStateBackend(new MemoryStateBackend());
        env.setParallelism(1); // 本地测试用单并行度

        // 2. 从Kafka读取页面日志数据（使用处理时间）
        tEnv.executeSql("CREATE TABLE ods_page_log (\n" +
                "  common MAP<STRING, STRING>,\n" +
                "  page MAP<STRING, STRING>,\n" +
                "  ts BIGINT,\n" +
                "  pt AS PROCTIME()  -- 处理时间字段\n" +
                ")" + SqlUtil.getKafka(
                DWD_TRAFFIC_PAGE_TOPIC, "retailersv_ods_page_log_vc_ch_ar")
        );

        // 3. 提取基础字段并计算当日日期
        Table baseTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "common['vc'] as vc,\n" +
                        "common['ch'] as ch,\n" +
                        "common['ar'] as ar,\n" +
                        "common['is_new'] as is_new,\n" +
                        "common['mid'] as mid,\n" +
                        "common['sid'] as sid,\n" +
                        "cast(page['during_time'] as BIGINT) as dur,\n" +
                        "pt,\n" +
                        "date_format(pt, 'yyyy-MM-dd') as cur_date_str\n" +
                        "FROM ods_page_log"
        );
        tEnv.createTemporaryView("base_table", baseTable);

        // 4. 计算UV标记（当日首次访问的用户计为1）
        Table uvTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "*, \n" +
                        "case when row_number() over(partition by mid, cur_date_str order by pt) = 1 then 1 else 0 end as uv_flag\n" +
                        "FROM base_table"
        );
        tEnv.createTemporaryView("uv_table", uvTable);

        // 5. 新增步骤：先计算窗口信息，避免在OVER子句中使用TUMBLE_START
        Table windowTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "*, \n" +
                        "tumble_start(pt, interval '5' second) as window_start,  -- 计算窗口起始时间\n" +
                        "tumble_end(pt, interval '5' second) as window_end      -- 计算窗口结束时间\n" +
                        "FROM table(tumble(table uv_table, descriptor(pt), interval '5' second))"
        );
        tEnv.createTemporaryView("window_table", windowTable);

        // 6. 计算SV标记（窗口内首次会话计为1）- 修复核心问题
        Table svTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "*, \n" +
                        // 使用预计算的window_start，避免在OVER子句中直接调用TUMBLE_START
                        "case when row_number() over(partition by sid, window_start order by pt) = 1 then 1 else 0 end as sv_flag\n" +
                        "FROM window_table"
        );
        tEnv.createTemporaryView("sv_table", svTable);

        // 7. 开窗聚合计算各项指标（5秒窗口）
        Table resultTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "date_format(window_start, 'yyyy-MM-dd HH:mm:ss') as stt,\n" +
                        "date_format(window_end, 'yyyy-MM-dd HH:mm:ss') as edt,\n" +
                        "date_format(window_start, 'yyyy-MM-dd') as cur_date,\n" +
                        "vc,\n" +
                        "ch,\n" +
                        "ar,\n" +
                        "cast(is_new as TINYINT) as is_new,\n" +
                        "sum(uv_flag) as uv_ct,\n" +
                        "sum(sv_flag) as sv_ct,\n" +
                        "count(*) as pv_ct,\n" +
                        "sum(dur) as dur_sum\n" +
                        "FROM sv_table \n" +
                        "GROUP BY window_start, window_end, vc, ch, ar, is_new "
        );
//         resultTable.execute().print();  // 调试用


        // 8. 创建MySQL Sink表（核心新增逻辑）
        // 修正后的 MySQL Sink 表创建语句（核心添加 PRIMARY KEY）
        String mysqlSinkSql = "CREATE TABLE mysql_traffic_core_sink (" +
                "  stt STRING, " +
                "  edt STRING, " +
                "  cur_date STRING, " +
                "  vc STRING, " +
                "  ch STRING, " +
                "  ar STRING, " +
                "  is_new TINYINT, " +
                "  uv_ct BIGINT, " +
                "  sv_ct BIGINT, " +
                "  pv_ct BIGINT, " +
                "  dur_sum BIGINT, " +
                // 声明主键（与 MySQL 表的主键完全一致）
                "  PRIMARY KEY (stt, edt, vc, ch, ar, is_new) NOT ENFORCED " +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://cdh01:3306/gmall2025?useSSL=false&serverTimezone=Asia/Shanghai'," +
                "  'username' = 'root'," +
                "  'password' = '123456'," +
                "  'table-name' = 'dws_traffic_core_indicator_window'," +
                "  'driver' = 'com.mysql.cj.jdbc.Driver'," +
                "  'sink.buffer-flush.max-rows' = '1'," +
                "  'sink.buffer-flush.interval' = '1000'" +
                ")";
        tEnv.executeSql(mysqlSinkSql);

        // 9. 将结果写入MySQL
        resultTable.executeInsert("mysql_traffic_core_sink");


    }
}
