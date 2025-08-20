package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

// 交易域退单明细事实表
public class DwdTradeOrderRefund {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_TRADE_ORDER_REFUND = ConfigUtils.getString("kafka.dwd.trade.order.refund");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setStateBackend(new MemoryStateBackend());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));


        tableEnv.executeSql("CREATE TABLE ods_ecommerce_order (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "   proc_time AS proctime()," +
                " time_ltz AS TO_TIMESTAMP_LTZ(ts_ms, 3),\n" +
                " WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "retailersv_dwd_trade_order_refund"));

        // 从 hbase 中读取 字典数据 创建 字典表
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")"+SqlUtil.getHbaseDDL("dim_base_dic"));

//        tableEnv.executeSql("select * from base_dic").print();

        // 2. 过滤退单表数据 order_refund_info   insert
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['user_id'] user_id," +
                        "`after`['order_id'] order_id," +
                        "`after`['sku_id'] sku_id," +
                        "MD5(CAST(`after`['refund_type'] AS STRING)) refund_type,\n" +
                        "`after`['refund_num'] refund_num," +
                        "`after`['refund_amount'] refund_amount," +
                        "MD5(CAST(`after`['refund_reason_type'] AS STRING)) refund_reason_type,\n" +
                        "`after`['refund_reason_txt'] refund_reason_txt," +
                        "`after`['create_time'] create_time," +
                        "proc_time," +
                        "ts_ms " +
                        "from ods_ecommerce_order " +
                        "where `source`['table'] ='order_refund_info' ");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

//        orderRefundInfo.execute().print();

        // 3. 过滤订单表中的退单数据: order_info  update
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['province_id'] province_id " +
                        " from ods_ecommerce_order " +
                        "where `source`['table']='order_info' " +
                        "and `before`['order_status'] is not null " +
                        "and `after`['order_status']='1005' ");
        tableEnv.createTemporaryView("order_info", orderInfo);

//        orderInfo.execute().print();
        // 4. join: 普通的和 lookup join

        Table result = tableEnv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "date_format(FROM_UNIXTIME(cast(ri.create_time as bigint) / 1000), 'yyyy-MM-dd') date_id," +
                        "ri.create_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name," +
                        "ri.refund_reason_type," +
                        "dic2.info.dic_name," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts_ms " +
                        "from order_refund_info ri " +
                        "join order_info oi " +
                        "on ri.order_id=oi.id " +
                        "join base_dic for system_time as of ri.proc_time as dic1 " +
                        "on ri.refund_type=dic1.dic_code " +
                        "join base_dic for system_time as of ri.proc_time as dic2 " +
                        "on ri.refund_reason_type=dic2.dic_code ");

        result.execute().print();

        // 5. 写出到 kafka
        tableEnv.executeSql(
                "create table "+DWD_TRADE_ORDER_REFUND+"(" +
                        "id string," +
                        "user_id string," +
                        "order_id string," +
                        "sku_id string," +
                        "province_id string," +
                        "date_id string," +
                        "create_time string," +
                        "refund_type_code string," +
                        "refund_type_name string," +
                        "refund_reason_type_code string," +
                        "refund_reason_type_name string," +
                        "refund_reason_txt string," +
                        "refund_num string," +
                        "refund_amount string," +
                        "ts bigint ," +
                        "primary key(id) not enforced " +
                        ")" + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_ORDER_REFUND));

        result.executeInsert(DWD_TRADE_ORDER_REFUND);
    }
}