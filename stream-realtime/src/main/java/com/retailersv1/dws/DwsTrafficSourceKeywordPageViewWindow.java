package com.retailersv1.dws;


import com.retailersv1.func.KwSplit;
import com.stream.common.utils.Constant;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.stream.common.utils.Constant.DORIS_DATABASE;
import static com.stream.common.utils.Constant.DORIS_FE_NODES;
//流量域搜索关键词粒度页面浏览各窗口汇总表

/**
 * 流量域来源关键词页面浏览窗口表（基于处理时间）
 * 功能：从Kafka读取页面浏览数据，过滤搜索行为，分词后统计各关键词出现频次，写入Doris
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    private static final String DWD_TRAFFIC_PAGE_TOPIC = Constant.TOPIC_DWD_TRAFFIC_PAGE;
    private static final String DWS_TRAFFIC_KEYWORD_SINK_IDENTIFIER =
            DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window";

    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 设置状态后端
        env.setStateBackend(new MemoryStateBackend());

        // 2. 从Kafka读取页面日志数据（使用处理时间，移除事件时间相关定义）
        tEnv.executeSql("CREATE TABLE ods_page_log (\n" +
                "  page MAP<STRING, STRING>,\n" +  // 页面信息
                "  ts BIGINT,\n" +  // 原始时间戳（仅保留，不用于时间语义）
                "  pt AS PROCTIME()  -- 定义处理时间字段（系统处理该条数据的时间）\n" +
                // 处理时间不需要水印（水印是事件时间的乱序处理机制）
                ")" + SqlUtil.getKafka(
                DWD_TRAFFIC_PAGE_TOPIC, "retailersv_ods_page_log")
        );

        // 3. 过滤搜索行为数据（保留处理时间字段pt）
        Table kwTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "page['item'] kw,\n" +
                        "pt  -- 改用处理时间字段\n" +
                        "FROM ods_page_log \n" +
                        "WHERE (page['last_page_id'] = 'search' OR page['last_page_id'] = 'home') \n" +
                        "AND page['item_type'] = 'keyword' \n" +
                        "AND page['item'] IS NOT NULL \n" +
                        "AND page['item'] <> ''"  // 排除空字符串
        );
//        kwTable.execute().print();
        tEnv.createTemporaryView("kw_table", kwTable);

        // 4. 注册并使用分词函数处理关键词（携带处理时间pt）
        tEnv.createTemporaryFunction("kw_split", KwSplit.class);
        Table keywordTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "keyword,\n" +
                        "pt  -- 保留处理时间用于后续窗口\n" +
                        "FROM kw_table \n" +
                        "JOIN lateral table(kw_split(kw)) ON TRUE "
        );
//        keywordTable.execute().print();
        tEnv.createTemporaryView("keyword_table", keywordTable);

        // 5. 基于处理时间开窗统计频次（5分钟窗口）
        Table resultTable = tEnv.sqlQuery(
                "SELECT \n" +
                        // 窗口起始时间（处理时间）
                        "date_format(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt,\n" +
                        // 窗口结束时间（处理时间）
                        "date_format(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt,\n" +
                        // 统计日期（基于窗口起始时间的处理时间）
                        "date_format(window_start, 'yyyyMMdd') AS cur_date,\n" +
                        "keyword,\n" +
                        "count(*) AS keyword_count \n" +
                        // 基于处理时间pt创建滚动窗口
                        "FROM table(tumble(table keyword_table, descriptor(pt), interval '1' SECOND  )) \n" +
                        "GROUP BY window_start, window_end, keyword"
        );
//        resultTable.execute().print();  // 调试用

        // 4. 创建 MySQL Sink 表（核心新增代码）
        String mysqlSinkSql = "CREATE TABLE mysql_keyword_sink (" +
                "  stt STRING, " +
                "  edt STRING, " +
                "  cur_date STRING, " +
                "  keyword STRING, " +
                "  keyword_count BIGINT " +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://cdh01:3306/gmall2025?useSSL=false&serverTimezone=Asia/Shanghai'," +  // 替换为你的MySQL地址和库名
                "  'username' = 'root'," +  // 如 root
                "  'password' = '123456'," +   // 如 123456
                "  'table-name' = 'dws_traffic_source_keyword_page_view_window'," +  // MySQL表名
                "  'driver' = 'com.mysql.cj.jdbc.Driver'," +  // MySQL 8.0+ 驱动类
                "  'sink.buffer-flush.max-rows' = '1'," +  // 测试用：1条数据就刷写
                "  'sink.buffer-flush.interval' = '1000' " +
        ")";
        tEnv.executeSql(mysqlSinkSql);

        // 5. 将 resultTable 写入 MySQL
        resultTable.executeInsert("mysql_keyword_sink");


    }
}