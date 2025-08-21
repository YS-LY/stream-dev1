package com.retailersv1.dws;


import com.stream.common.utils.Constant;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//12.2流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
/**
 * 流量域版本渠道地区新老访客页面浏览窗口表
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

        // 2. 从Kafka读取页面日志数据并设置水位线
        tEnv.executeSql("CREATE TABLE ods_page_log (\n" +
                "  common MAP<STRING, STRING>,\n" +
                "  page MAP<STRING, STRING>,\n" +
                "  ts BIGINT,\n" +
                "  et AS to_timestamp_ltz(ts, 3),\n" +
                "  watermark for et as et - interval '5' second \n" +  // 允许5秒延迟
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
                        "et  ,\n" +
                        "date_format(et, 'yyyy-MM-dd') as cur_date_str\n" +
                        "FROM ods_page_log"
        );
//        baseTable.execute().print();
        tEnv.createTemporaryView("base_table", baseTable);

        // 4. 计算UV标记（当日首次访问的用户计为1）
        Table uvTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "*, \n" +
                        "case when row_number() over(partition by mid, cur_date_str order by et) = 1 then 1 else 0 end as uv_flag\n" +
                        "FROM base_table"
        );
        uvTable.execute().print();
        tEnv.createTemporaryView("uv_table", uvTable);

        // 5. 计算SV标记（窗口内首次会话计为1）
        Table svTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "*, \n" +
                        "case when row_number() over(partition by sid, tumble_start(et, interval '5' second) order by et) = 1 then 1 else 0 end as sv_flag\n" +
                        "FROM uv_table"
        );
        tEnv.createTemporaryView("sv_table", svTable);

        // 6. 开窗聚合计算各项指标（5秒窗口）
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
                        "FROM table(tumble(table sv_table, descriptor(et), interval '5' second )) \n" +
                        "GROUP BY window_start, window_end, vc, ch, ar, is_new "
        );
        // resultTable.execute().print();

        // 7. 创建Doris Sink表
        String createSinkSql = "CREATE TABLE dws_traffic_vc_ch_ar_is_new_page_view_window (\n" +
                "stt DATETIME COMMENT '窗口起始时间',\n" +
                "edt DATETIME COMMENT '窗口结束时间',\n" +
                "cur_date DATE COMMENT '当天日期',\n" +
                "vc VARCHAR(256) COMMENT '版本号',\n" +
                "ch VARCHAR(256) COMMENT '渠道',\n" +
                "ar TINYINT COMMENT '地区',\n" +
                "is_new TINYINT COMMENT '新老访客状态标记',\n" +
                "uv_ct BIGINT REPLACE COMMENT '独立访客数',\n" +
                "sv_ct BIGINT REPLACE COMMENT '会话数',\n" +
                "pv_ct BIGINT REPLACE COMMENT '页面浏览数',\n" +
                "dur_sum BIGINT REPLACE COMMENT '累计访问时长'\n" +
                ")" + "WITH (\n" +
                " 'connector' = 'doris',\n" +
                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "',\n" +
                " 'table.identifier' = '" + DWS_TRAFFIC_VC_CH_AR_IS_NEW_SINK_IDENTIFIER + "',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456', \n" +
                " 'sink.properties.format' = 'json', \n" +
                " 'sink.buffer-count' = '4', \n" +
                " 'sink.buffer-size' = '4086',\n" +
                " 'sink.enable-2pc' = 'true', \n" +
                " 'sink.properties.read_json_by_line' = 'true' \n" +
                ")";
        tEnv.executeSql(createSinkSql);

        // 8. 将结果写入Doris
        resultTable.executeInsert("dws_traffic_vc_ch_ar_is_new_page_view_window");
    }
}
