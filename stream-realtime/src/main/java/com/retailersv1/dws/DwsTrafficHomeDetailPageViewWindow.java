package com.retailersv1.dws;

import com.stream.common.utils.Constant;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 流量域首页和商品详情页浏览窗口表（基于处理时间）
 * 功能：从Kafka读取页面浏览数据，统计各窗口内首页和详情页独立访客数
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
        env.setParallelism(1); // 本地测试用单并行度

        // 2. 从Kafka读取页面日志数据（使用处理时间）
        tEnv.executeSql("CREATE TABLE ods_page_log (\n" +
                "  common MAP<STRING, STRING>,\n" +  // 公共信息MAP
                "  page MAP<STRING, STRING>,\n" +    // 页面信息MAP
                "  ts BIGINT,\n" +                  // 原始时间戳（仅保留，不用于时间语义）
                "  pt AS PROCTIME()                  // 定义处理时间字段（系统处理该条数据的时间）\n" +
                ")" + SqlUtil.getKafka(
                DWD_TRAFFIC_PAGE_TOPIC, "dws_traffic_home_detail_page_view_source")
        );

        // 3. 过滤首页和商品详情页数据，并提取关键字段（使用处理时间）
        Table filteredTable = tEnv.sqlQuery(
                "SELECT " +
                        "common['mid'] AS mid, " +  // 设备唯一标识（用于计算UV）
                        "page['page_id'] AS page_id, " +
                        "pt  " +  // 携带处理时间字段用于后续窗口计算
                        "FROM ods_page_log " +
                        "WHERE page['page_id'] IN ('home', 'good_detail') " +  // 仅保留首页和详情页
                        "AND common['mid'] IS NOT NULL " +  // 过滤无效设备ID
                        "AND page['page_id'] IS NOT NULL"    // 过滤无效页面ID
        );
//         filteredTable.execute().print(); // 调试用
        tEnv.createTemporaryView("filtered_table", filteredTable);

        // 4. 基于处理时间开窗聚合，计算各窗口独立访客数
        Table resultTable = tEnv.sqlQuery(
                "SELECT " +
                        "date_format(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt, " +  // 窗口起始时间
                        "date_format(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt, " +    // 窗口结束时间
                        "date_format(window_start, 'yyyy-MM-dd' ) AS cur_date, " +        // 统计日期（基于处理时间窗口）
                        // 首页独立访客数（按mid去重）
                        "COUNT(DISTINCT CASE WHEN page_id = 'home' THEN mid ELSE NULL END) AS home_uv_ct, " +
                        // 商品详情页独立访客数（按mid去重）
                        "COUNT(DISTINCT CASE WHEN page_id = 'good_detail' THEN mid ELSE NULL END) AS good_detail_uv_ct " +
                        // 基于处理时间pt创建5分钟滚动窗口（原5秒改为5分钟，更符合实际业务）
                        "FROM table(tumble(table filtered_table, descriptor(pt), interval '5' SECOND)) " +
                        "GROUP BY window_start, window_end"  // 按窗口分组
        );
//         resultTable.execute().print();  // 调试用


        // 5. 创建MySQL Sink表
        String mysqlSinkSql = "CREATE TABLE mysql_home_detail_sink (" +
                "  stt STRING, " +
                "  edt STRING, " +
                "  cur_date STRING, " +
                "  home_uv_ct BIGINT, " +
                "  good_detail_uv_ct BIGINT " +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://cdh01:3306/gmall2025?useSSL=false&serverTimezone=Asia/Shanghai'," +
                "  'username' = 'root'," +  // 如root
                "  'password' = '123456'," +   // 如123456
                "  'table-name' = 'dws_traffic_home_detail_page_view_window'," +  // 目标表名
                "  'driver' = 'com.mysql.cj.jdbc.Driver'," +  // MySQL驱动
                "  'sink.buffer-flush.max-rows' = '1'," +  // 测试用：1条即提交
                "  'sink.buffer-flush.interval' = '1000' "+ // 1秒强制刷新
        ")";
        tEnv.executeSql(mysqlSinkSql);

        // 6. 将结果写入MySQL
        resultTable.executeInsert("mysql_home_detail_sink");

    }
}
