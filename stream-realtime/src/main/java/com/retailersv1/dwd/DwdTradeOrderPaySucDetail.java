package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// 交易域支付成功明细事实表
public class DwdTradeOrderPaySucDetail {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_ORDER_DETAIL_TOPIC = ConfigUtils.getString("kafka.dwd.trade.order.detail");
    private static final String DWD_ORDER_PAY_SUC_DETAIL_TOPIC = ConfigUtils.getString("kafka.dwd.trade.order.pay.suc.detail");

    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setStateBackend(new MemoryStateBackend());

        // 2. 读取下单事务事实表（dwd_trade_order_detail）
        tEnv.executeSql("CREATE TABLE dwd_trade_order_detail (\n" +
                        "detail_id STRING,\n" +
                        "order_id STRING,\n" +
                        "user_id STRING,\n" +
                        "sku_id STRING,\n" +
                        "sku_name STRING,\n" +
                        "province_id STRING,\n" +
                        "activity_id STRING,\n" +
                        "activity_rule_id STRING,\n" +
                        "coupon_id STRING,\n" +
                        "date_id STRING,\n" +
                        "create_time STRING,\n" +
                        "sku_num STRING,\n" +
                        "split_original_amount STRING,\n" +
                        "split_activity_amount STRING,\n" +
                        "split_coupon_amount STRING,\n" +
                        "split_total_amount STRING,\n" +
                        "ts BIGINT\n" +
//                        "et AS to_timestamp_ltz(ts, 0),\n" +  // 事件时间
//                        "watermark FOR et AS et - INTERVAL '3' SECOND \n"+
                ")" + SqlUtil.getKafka(DWD_ORDER_DETAIL_TOPIC, "retailersv_dwd_order_detail_consumer")
        );
//        tEnv.executeSql("select * from dwd_trade_order_detail").print();

        // 3. 读取ODS层topic_db数据（包含支付表）
        tEnv.executeSql("CREATE TABLE ods_ecommerce_payment (\n" +
                        "  `op` STRING,\n" +
                        "  `before` MAP<STRING,STRING>,\n" +
                        "  `after` MAP<STRING,STRING>,\n" +
                        "  `source` MAP<STRING,STRING>,\n" +
                        "  `ts_ms` BIGINT,\n" +
                        "  proc_time AS proctime()  \n"+
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "retailersv_ods_ecommerce_payment")
        );
//        tEnv.executeSql("select * from ods_ecommerce_payment where `source`['table'] = 'payment_info' ").print();

        // 4. 读取字典表（维表）
        tEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")"+SqlUtil.getHbaseDDL("dim_base_dic"));
//        tEnv.executeSql("select * from base_dic").print();

        // 5. 筛选支付成功数据
        Table paymentInfo = tEnv.sqlQuery("SELECT\n" +
                        "`after`['user_id'] AS user_id,\n" +
                        "`after`['order_id'] AS order_id,\n" +
                        "MD5(CAST(`after`['payment_type'] AS STRING)) payment_type,\n" +
                        "`after`['callback_time'] AS callback_time,\n" +
                        "proc_time AS pt,\n" +  // 处理时间（用于维表关联）
                        "ts_ms AS pay_ts,\n" +  // 支付时间戳
                        "to_timestamp_ltz(ts_ms, 0) AS pay_et \n"+
                        "FROM ods_ecommerce_payment\n" +
                        "WHERE `source`['table'] = 'payment_info'\n" +
                        "AND `op` = 'u'\n" +
                        "AND `before`['payment_status'] IS NOT NULL\n" +
                        "AND `after`['payment_status'] = '1602'"  // 1602表示支付成功
        );
//        paymentInfo.execute().print();
        tEnv.createTemporaryView("payment_info", paymentInfo);

        // 6. 三表关联（订单明细、支付信息、字典表）
        Table result = tEnv.sqlQuery("SELECT\n" +
                "od.detail_id AS order_detail_id,\n" +
                "od.order_id,\n" +
                "od.user_id,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "od.province_id,\n" +
                "od.activity_id,\n" +
                "od.activity_rule_id,\n" +
                "od.coupon_id,\n" +
                "pi.payment_type AS payment_type_code,\n" +
                "dic.dic_name AS payment_type_name,\n" +
                "pi.callback_time AS pay_success_time,\n" +
                "od.sku_num,\n" +
                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount AS split_payment_amount,\n" +
                "pi.pay_ts AS ts\n" +
                "FROM payment_info pi\n" +
                // 基于事件时间的区间关联（使用dwd_trade_order_detail的水印）
                "JOIN dwd_trade_order_detail od\n" +
                "ON pi.order_id = od.order_id\n" +
//                "AND od.et BETWEEN pi.pay_et - INTERVAL '15' MINUTE AND pi.pay_et + INTERVAL '5' SECOND\n" +
                // 维表关联（使用处理时间）
                "JOIN base_dic FOR SYSTEM_TIME AS OF pi.pt AS dic\n" +
                "ON pi.payment_type = dic.dic_code"
        );
        result.execute().print();

        tEnv.executeSql("CREATE TABLE dwd_trade_order_pay_suc_detail (\n" +
                "order_detail_id STRING,\n" +
                "order_id STRING,\n" +
                "user_id STRING,\n" +
                "sku_id STRING,\n" +
                "sku_name STRING,\n" +
                "province_id STRING,\n" +
                "activity_id STRING,\n" +
                "activity_rule_id STRING,\n" +
                "coupon_id STRING,\n" +
                "payment_type_code STRING,\n" +
                "payment_type_name STRING,\n" +
                "pay_success_time STRING,\n" +
                "sku_num STRING,\n" +
                "split_original_amount STRING,\n" +
                "split_activity_amount STRING,\n" +
                "split_coupon_amount STRING,\n" +
                "split_payment_amount STRING,\n" +
                "ts BIGINT,\n" +
                "PRIMARY KEY (order_detail_id) NOT ENFORCED\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(DWD_ORDER_PAY_SUC_DETAIL_TOPIC));

        // 8. 写入Kafka
        result.executeInsert("dwd_trade_order_pay_suc_detail");
    }
}