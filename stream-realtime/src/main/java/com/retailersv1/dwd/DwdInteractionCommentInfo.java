package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//互动域评论事务事实表
public class DwdInteractionCommentInfo {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_COMMENT_INFO = ConfigUtils.getString("kafka.dwd.comment.info");


    public static void main(String[] args) throws Exception {
        // 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 创建ODS层Kafka源表（消费评论表CDC数据）
        tEnv.executeSql("CREATE TABLE ods_ecommerce_order (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "   proc_time AS proctime()" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "retailersv_ods_ecommerce_order"));
//        tEnv.executeSql("select * from ods_ecommerce_order where `source`['table'] = 'comment_info' ").print();



        Table commentInfo = tEnv.sqlQuery("select\n" +
                "`after`['id'] id,\n" +
                "`after`['user_id'] user_id,\n" +
                "`after`['sku_id'] sku_id,\n" +
                "MD5(CAST(`after`['appraise'] AS STRING)) appraise,\n" +
                "`after`['comment_txt'] comment_txt,\n" +
                "ts_ms,\n" +
                "proc_time\n" +
                "from ods_ecommerce_order\n" +
                "where `source`['table'] = 'comment_info'");
//        commentInfo.execute().print();
        // 将表对象注册到表执行环境中
        tEnv.createTemporaryView("comment_info", commentInfo);


        tEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")"+SqlUtil.getHbaseDDL("dim_base_dic"));
//        tEnv.executeSql("select * from base_dic").print();

        // 将评论表和字典表进行关联
        Table joinTable = tEnv.sqlQuery("SELECT id,\n" +
                "      user_id,\n" +
                "      sku_id,\n" +
                "      appraise,\n" +
                "      dic.dic_name appraise_name,\n" +
                "      comment_txt,\n" +
                "      ts_ms\n" +
                "FROM comment_info AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time AS dic\n" +
                "    ON c.appraise = dic.dic_code");
        joinTable.execute().print();


        // 将关联后的表数据写入 kafka
        tEnv.executeSql("CREATE TABLE "+DWD_COMMENT_INFO+" (\n" +
                "      id String,\n" +
                "      user_id string,\n" +
                "      sku_id string,\n" +
                "      appraise CHAR(32),\n" +
                "      appraise_name string,\n" +
                "      comment_txt string,\n" +
                "      ts BIGINT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"+SqlUtil.getUpsertKafkaDDL(DWD_COMMENT_INFO));
// 6. 唯一触发点：写入目标表
        joinTable.executeInsert(DWD_COMMENT_INFO);



    }
}