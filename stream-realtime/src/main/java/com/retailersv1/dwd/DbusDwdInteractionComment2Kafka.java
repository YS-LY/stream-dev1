package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 评论事实表处理：读取Kafka评论数据，关联HBase维度表，完成维度退化后写入DWD层Kafka
 */
public class DbusDwdInteractionComment2Kafka {
    // 配置参数：Kafka源表主题（评论数据）、DWD输出主题、HBase维度表名
    private static final String ODS_COMMENT_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_COMMENT_TOPIC = ConfigUtils.getString("kafka.dwd.interaction.comment.topic");
    private static final String HBASE_DIM_TABLE = ConfigUtils.getString("hbase.dim.base_dic"); // 如：dim_base_dic

    public static void main(String[] args) {
        // 1. 初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env); // 配置并行度、检查点等
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 创建Kafka源表（读取ODS层评论JSON数据）
        tableEnv.executeSql("CREATE TABLE ods_interaction_comment (\n" +
                        "  `id` STRING,\n" +                // 评论ID
                        "  `user_id` STRING,\n" +          // 用户ID
                        "  `sku_id` STRING,\n" +           // 商品ID
                        "  `appraise` STRING,\n" +         // 评价等级编码（如1-好评，2-中评，需关联维度）
                        "  `comment_txt` STRING,\n" +      // 评论内容
                        "  `create_time` STRING,\n" +      // 评论时间
                        "  `ts` BIGINT,\n" +               // 时间戳

                ")" + SqlUtil.getKafka(ODS_COMMENT_TOPIC, "dwd_comment_consumer_group"));

        // 3. 创建HBase维度表（评价等级字典表，用于维度退化）
        // 假设HBase表结构：rowkey=appraise_code，列族info包含dic_name（评价等级名称）
        tableEnv.executeSql("CREATE TABLE hbase_dim_appraise (\n" +
                "  `appraise_code` STRING,\n" +    // 评价等级编码（与评论表appraise关联）
                "  `appraise_name` STRING,\n" +    // 评价等级名称（如"好评"）
                "  PRIMARY KEY (`appraise_code`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'hbase-1.4',\n" +
                "  'table-name' = '" + ConfigUtils.getString("hbase.namespace") + ":" + HBASE_DIM_TABLE + "',\n" + // 完整表名：namespace:table
                "  'zookeeper.quorum' = '" + ConfigUtils.getString("zookeeper.server.host.list") + "',\n" + // ZK地址
                "  'zookeeper.znode.parent' = '/hbase'\n" + // HBase在ZK中的根节点
                ")");

        // 4. 关联评论数据与HBase维度表（维度退化：补充评价等级名称）
        Table dwdCommentTable = tableEnv.sqlQuery("SELECT\n" +
                "  c.id,\n" +
                "  c.user_id,\n" +
                "  c.sku_id,\n" +
                "  c.appraise AS appraise_code,\n" +  // 保留编码
                "  d.appraise_name,\n" +             // 维度退化：补充名称
                "  c.comment_txt,\n" +
                "  c.create_time AS comment_time,\n" +
                "  c.ts\n" +
                "FROM ods_interaction_comment c\n" +
                "LEFT JOIN hbase_dim_appraise FOR SYSTEM_TIME AS OF c.proc_time AS d\n" +  // Lookup Join（处理时间关联）
                "ON c.appraise = d.appraise_code");  // 关联条件：评价编码

        // 5. 创建DWD层Upsert-Kafka表（用于写入结果）
        tableEnv.executeSql("CREATE TABLE dwd_interaction_comment (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  appraise_code STRING,\n" +
                "  appraise_name STRING,\n" +  // 已退化的维度字段
                "  comment_txt STRING,\n" +
                "  comment_time STRING,\n" +
                "  ts BIGINT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED  // 以评论ID为主键，支持Upsert\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(DWD_COMMENT_TOPIC));

        // 6. 将关联结果写入DWD层Kafka
        dwdCommentTable.executeInsert("dwd_interaction_comment");
    }
}

