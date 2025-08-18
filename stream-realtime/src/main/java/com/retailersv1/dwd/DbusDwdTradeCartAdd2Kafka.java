package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.retailersv
 * @Author xiaoye
 * @Date 2025/8/18 18:37
 * @description: 完成加购事实表
 */
public class DbusDwdTradeCartAdd2Kafka {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_CART_INFO = ConfigUtils.getString("kafka.dwd.cart.info");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE ods_ecommerce_order (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "   proc_time AS proctime()" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "retailersv_ods_ecommerce_order"));
//        tableEnv.executeSql("select * from ods_ecommerce_order where `source`['table'] = 'cart_info ").print();

        Table cartInfo = tableEnv.sqlQuery("select\n" +
                "`after`['id'] id,\n" +
                "`after`['user_id'] user_id,\n" +
                "`after`['sku_id'] sku_id,\n" +
                " if(`op` = 'r',cast(`after`['sku_num'] as int),(cast(`after`['sku_num'] as int) - cast(`before`['sku_num'] as int))) sku_num,\n" +
                "ts_ms\n" +
                "from ods_ecommerce_order\n" +
                "where `source`['table'] = 'cart_info'\n" +
                "and (\n" +
                "(`op` = 'r')\n" +
                "or\n" +
                "(`op` = 'u' and `before`['sku_num'] is not null and (cast(`after`['sku_num'] as int) > cast(`before`['sku_num'] as int)) )\n" +
                ")");
        cartInfo.execute().print();

        tableEnv.executeSql("CREATE TABLE "+DWD_CART_INFO+" (\n" +
                "      id String,\n" +
                "      user_id string,\n" +
                "      sku_id string,\n" +
                "      sku_num INT,\n" +
                "      ts BIGINT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"+SqlUtil.getUpsertKafkaDDL(DWD_CART_INFO));
        cartInfo.executeInsert(DWD_CART_INFO);

    }

}
