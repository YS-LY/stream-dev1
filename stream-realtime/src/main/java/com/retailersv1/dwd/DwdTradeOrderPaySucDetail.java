package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// 交易域支付成功明细事实表


/**
 * @Package com.retailersv.dwd
 * @Author xiaoye
 * @Date 2025/8/19 10:02
 * @description: 交易域支付成功事实表
 */
public class DwdTradeOrderPaySucDetail {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_TRADE_ORDER_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.detail");
    private static final String DWD_TRADE_ORDER_PAYMENT_SUCCESS = ConfigUtils.getString("kafka.dwd.trade.order.pay.suc.detail");


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setStateBackend(new MemoryStateBackend());

        tableEnv.executeSql("CREATE TABLE ods_ecommerce_order (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "   proc_time AS proctime()," +
                " time_ltz AS TO_TIMESTAMP_LTZ(ts_ms, 3),\n" +
                " WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "retailersv_dwd_trade_payment_success"));
        // 1. 读取下单事务事实表
        tableEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint ," +
                        " time_ltz AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                        " WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND" +
                        ")" + SqlUtil.getKafka(DWD_TRADE_ORDER_DETAIL, "retailersv_dwd_trade_payment_success"));

//        tableEnv.executeSql("select * from dwd_trade_order_detail").print();

        // 2. 从 ods_ecommerce_order 中过滤 payment_info
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "`after`['user_id'] user_id," +
                "`after`['order_id'] order_id," +
                "MD5(CAST(`after`['payment_type'] AS STRING)) payment_type,\n" +
                "`after`['callback_time'] callback_time," +
                "`proc_time`," +
                "ts_ms, " +
                "time_ltz " +
                "from ods_ecommerce_order " +
                "where `source`['table'] = 'payment_info' " +
                "and `op` = 'u' " +
                "and `after`['payment_status']='1602' ");
        tableEnv.createTemporaryView("payment_info", paymentInfo);

//        paymentInfo.execute().print();

        //3 从 hbase 中读取 字典数据 创建 字典表
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")"+SqlUtil.getHbaseDDL("dim_base_dic"));

//        tableEnv.executeSql("select * from base_dic").print();

        // 4. 3张join: interval join 无需设置 ttl
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code ," +
                        "dic.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts_ms " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id=od.order_id " +
                        "and od.time_ltz >= pi.time_ltz - interval '30' minute " +
                        "and od.time_ltz <= pi.time_ltz + interval '5' second " +
                        "join base_dic for system_time as of pi.proc_time as dic " +
                        "on pi.payment_type=dic.dic_code ");

//        result.execute().print();

        // 6. 写出到 kafka 中
        tableEnv.executeSql("create table "+DWD_TRADE_ORDER_PAYMENT_SUCCESS+"(" +
                "order_detail_id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "callback_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "ts bigint ," +
                "primary key(order_detail_id) not enforced " +
                ")" + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        result.executeInsert(DWD_TRADE_ORDER_PAYMENT_SUCCESS);

    }
}