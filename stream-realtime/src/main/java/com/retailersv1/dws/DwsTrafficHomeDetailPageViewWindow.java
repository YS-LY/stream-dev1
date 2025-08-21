package com.retailersv1.dws;

import com.stream.common.utils.Constant;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//12.3流量域首页、详情页页面浏览各窗口汇总表
/**
 * 流量域首页和商品详情页浏览窗口表
 * 功能：从Kafka读取页面浏览数据，统计各窗口内首页和详情页独立访客数，写入Doris
 */
public class DwsTrafficHomeDetailPageViewWindow {
    private static final String DWD_TRAFFIC_PAGE_TOPIC = Constant.TOPIC_DWD_TRAFFIC_PAGE;
    private static final String DWS_TRAFFIC_HOME_DETAIL_SINK_IDENTIFIER =
            Constant.DORIS_DATABASE + ".dws_traffic_home_detail_page_view_window";

    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 设置状态后端（生产环境建议使用RocksDBStateBackend）
        env.setStateBackend(new MemoryStateBackend());

        // 2. 从Kafka读取页面日志数据并设置水位线
        tEnv.executeSql("CREATE TABLE ods_page_log (\n" +
                "  common MAP<STRING, STRING>,\n" +
                "  page MAP<STRING, STRING>,\n" +
                "  ts BIGINT,\n" +
                "  et AS to_timestamp_ltz(ts, 3),\n" +
                "  watermark for et as et - interval '5' second \n" +  // 允许5秒延迟
                ")" + SqlUtil.getKafka(
                DWD_TRAFFIC_PAGE_TOPIC, "dws_traffic_home_detail_page_view_source")
        );

        // 3. 过滤首页和商品详情页数据，并提取关键字段
        Table filteredTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "common['mid'] mid,\n" +  // 设备唯一标识（用于计算UV）
                        "page['page_id'] page_id,\n" +
                        "date_format(et, 'yyyyMMdd') cur_date,\n" +
                        "et \n" +
                        "FROM ods_page_log \n" +
                        "WHERE page['page_id'] IN ('home', 'good_detail') \n" +
                        "AND common['mid'] IS NOT NULL \n" +
                        "AND page['page_id'] IS NOT NULL"
        );
//        filteredTable.execute().print();
        tEnv.createTemporaryView("filtered_table", filteredTable);

        // 4. 直接开窗聚合，在聚合中完成去重（避免单独去重产生变更流）
        Table resultTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                        "date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                        "date_format(window_start, 'yyyyMMdd') cur_date,\n" +
                        // 窗口内首页独立访客数（按mid去重）
                        "COUNT(DISTINCT CASE WHEN page_id = 'home' THEN mid ELSE NULL END) AS home_uv_ct,\n" +
                // 窗口内商品详情页独立访客数（按mid去重）
                "COUNT(DISTINCT CASE WHEN page_id = 'good_detail' THEN mid ELSE NULL END) AS good_detail_uv_ct \n" +
                        "FROM table(tumble(table filtered_table, descriptor(et), interval '5' second )) \n" +  // 5秒窗口
                        "GROUP BY window_start, window_end"
        );
         resultTable.execute().print();  // 调试用

        // 5. 创建Doris Sink表
        String createSinkSql = "CREATE TABLE dws_traffic_home_detail_page_view_window (\n" +
                "stt STRING COMMENT '窗口起始时间',\n" +
                "edt STRING COMMENT '窗口结束时间',\n" +
                "cur_date STRING COMMENT '当天日期',\n" +
                "home_uv_ct BIGINT COMMENT '首页独立访客数',\n" +
                "good_detail_uv_ct BIGINT COMMENT '商品详情页独立访客数'\n" +
                ")" + "WITH (\n" +
                " 'connector' = 'doris',\n" +
                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "',\n" +
                " 'table.identifier' = '" + DWS_TRAFFIC_HOME_DETAIL_SINK_IDENTIFIER + "',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456', \n" +
                " 'sink.properties.format' = 'json', \n" +
                " 'sink.buffer-count' = '4', \n" +
                " 'sink.buffer-size' = '4086',\n" +
                " 'sink.enable-2pc' = 'true', \n" +
                " 'sink.properties.read_json_by_line' = 'true' \n" +
                ")";
        tEnv.executeSql(createSinkSql);

        // 6. 将结果写入Doris
        resultTable.executeInsert("dws_traffic_home_detail_page_view_window");
    }
}