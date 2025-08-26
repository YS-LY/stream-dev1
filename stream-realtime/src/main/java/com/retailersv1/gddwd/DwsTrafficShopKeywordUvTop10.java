package com.retailersv1.gddwd;

import com.retailersv1.func.KwSplit;
import com.stream.common.utils.Constant;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.stream.common.utils.Constant.DORIS_DATABASE;

/**
 * 流量域店铺搜索关键词访客数Top10窗口表（基于处理时间）
 * 功能：从Kafka读取搜索数据，按店铺维度统计各窗口内访客数最高的10个关键词
 */
public class DwsTrafficShopKeywordUvTop10 {
    private static final String DWD_TRAFFIC_PAGE_TOPIC = Constant.TOPIC_DWD_TRAFFIC_PAGE;
    private static final String DWS_TRAFFIC_SHOP_KEYWORD_SINK =
            DORIS_DATABASE + ".dws_traffic_shop_keyword_uv_top10";

    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 设置状态后端（生产环境建议使用RocksDB）
        env.setStateBackend(new MemoryStateBackend());
        env.setParallelism(1); // 本地测试用单并行度

        // 2. 从Kafka读取页面日志数据（保留处理时间语义）
        tEnv.executeSql("CREATE TABLE ods_page_log (\n" +
                        "  common MAP<STRING, STRING>,\n" +  // 公共信息（包含设备ID）
                        "  page MAP<STRING, STRING>,\n" +    // 页面信息（包含关键词和店铺ID）
                        "  ts BIGINT,\n" +                  // 原始时间戳
                        "  pt AS PROCTIME() "+                 // 处理时间字段
                ")" + SqlUtil.getKafka(
                        DWD_TRAFFIC_PAGE_TOPIC, "retailersv_shop_keyword_source")
        );

        // 3. 过滤店铺搜索相关数据（提取设备ID、店铺ID、关键词）
        Table shopSearchTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "common['mid'] AS mid,  -- 设备唯一标识（用于计算UV）\n" +
                        "page['shop_id'] AS shop_id,  -- 店铺ID\n" +
                        "page['item'] AS kw,  -- 搜索关键词\n" +
                        "pt  -- 处理时间\n" +
                        "FROM ods_page_log \n" +
                        "WHERE page['last_page_id'] = 'search' \n" +  // 仅保留搜索来源页面
                        "AND page['item_type'] = 'keyword' \n" +     // 关键词类型
                        "AND page['shop_id'] IS NOT NULL \n" +      // 过滤无店铺ID的记录
                        "AND page['item'] IS NOT NULL \n" +         // 过滤无关键词的记录
                        "AND page['item'] <> '' \n" +
                        "AND common['mid'] IS NOT NULL"             // 过滤无效设备ID
        );
        tEnv.createTemporaryView("shop_search_table", shopSearchTable);

        // 4. 使用分词函数处理关键词（保留处理时间和店铺维度）
        tEnv.createTemporaryFunction("kw_split", KwSplit.class);
        Table splitKeywordTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "mid, \n" +
                        "shop_id, \n" +
                        "keyword, \n" +
                        "pt \n" +
                        "FROM shop_search_table \n" +
                        "JOIN lateral table(kw_split(kw)) ON TRUE "  // 沿用原分词逻辑
        );
        tEnv.createTemporaryView("split_keyword_table", splitKeywordTable);

        // 5. 基于处理时间开窗，统计各店铺各关键词的独立访客数
        Table keywordUvTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "shop_id, \n" +
                        "keyword, \n" +
                        "window_start, \n" +
                        "window_end, \n" +
                        "COUNT(DISTINCT mid) AS uv_count,  -- 统计独立访客数\n" +
                        "date_format(window_start, 'yyyyMMdd') AS cur_date \n" +
                        "FROM table(tumble(table split_keyword_table, descriptor(pt), interval '5' MINUTE)) \n" +  // 5分钟窗口
                        "GROUP BY shop_id, keyword, window_start, window_end"
        );
        tEnv.createTemporaryView("keyword_uv_table", keywordUvTable);

        // 6. 计算每个店铺每个窗口的关键词Top10（按访客数排序）
        Table top10Table = tEnv.sqlQuery(
                "SELECT \n" +
                        "shop_id, \n" +
                        "keyword, \n" +
                        "uv_count, \n" +
                        "cur_date, \n" +
                        "date_format(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt, \n" +
                        "date_format(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt, \n" +
                        "rank_num \n" +
                        "FROM (\n" +
                        "  SELECT \n" +
                        "    *, \n" +
                        "    ROW_NUMBER() OVER (\n" +
                        "      PARTITION BY shop_id, window_start \n" +  // 按店铺和窗口分区
                        "      ORDER BY uv_count DESC \n" +             // 按访客数降序
                        "    ) AS rank_num \n" +
                        "  FROM keyword_uv_table\n" +
                        ") t \n" +
                        "WHERE rank_num <= 10"  // 取Top10关键词
        );

        // 7. 创建MySQL Sink表
        String mysqlSinkSql = "CREATE TABLE mysql_shop_keyword_top10_sink (\n" +
                "  shop_id STRING, \n" +
                "  keyword STRING, \n" +
                "  uv_count BIGINT, \n" +
                "  cur_date STRING, \n" +
                "  stt STRING, \n" +
                "  edt STRING, \n" +
                "  rank_num INT \n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://cdh01:3306/gmall2025?useSSL=false&serverTimezone=Asia/Shanghai',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'table-name' = 'dws_traffic_shop_keyword_uv_top10',\n" +
                "  'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "  'sink.buffer-flush.max-rows' = '100',\n" +  // 批量写入优化
                "  'sink.buffer-flush.interval' = '5000' \n" +
                ")";
        tEnv.executeSql(mysqlSinkSql);

        // 8. 将结果写入MySQL
        top10Table.executeInsert("mysql_shop_keyword_top10_sink");
    }
}

