package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

// 交易域退款支付成功明细事实表
public class DwdTradeRefundPaySucDetail {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_TRADE_REFUND_PAYMENT_SUCCESS_TOPIC = ConfigUtils.getString("kafka.dwd.trade.refund.pay.suc.detail");

    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setStateBackend(new MemoryStateBackend());

        // 设置TTL为5秒，处理可能的数据乱序
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // 2. 创建ODS层源表（复用Kafka CDC主题）
        tEnv.executeSql("CREATE TABLE ods_ecommerce_db (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "  proc_time AS proctime()\n" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "retailersv_ods_trade_refund_pay_suc")
        );

        // 从 hbase 中读取 字典数据 创建 字典表
        tEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")"+SqlUtil.getHbaseDDL("dim_base_dic"));

        // 4. 提取各表数据并创建临时视图
        // 4.1 筛选退款成功的退款表数据（主表）
        Table refundPayment = tEnv.sqlQuery(
                "select " +
                        "`after`['id'] as id," +
                        "`after`['order_id'] as order_id," +
                        "`after`['sku_id'] as sku_id," +
                        "MD5(CAST(`after`['payment_type'] AS STRING)) as payment_type," +
                        "`after`['callback_time'] as callback_time," +
                        "`after`['total_amount'] as total_amount," +
                        "proc_time as pt," +
                        "ts_ms as ts " +
                        "from ods_ecommerce_db " +
                        "where `source`['table'] = 'refund_payment' " +
                        "and `op` = 'u' " +  // 操作类型为update
                        "and `before`['refund_status'] is not null " +  // 确保修改了refund_status字段
                        "and `after`['refund_status'] = '1602' " +  // 退款状态为已支付
                        "and `after` is not null"
        );
        tEnv.createTemporaryView("refund_payment", refundPayment);

        // 4.2 筛选退款成功的退单表数据
        Table orderRefundInfo = tEnv.sqlQuery(
                "select " +
                        "`after`['order_id'] as order_id," +
                        "`after`['sku_id'] as sku_id," +
                        "`after`['refund_num'] as refund_num " +
                        "from ods_ecommerce_db " +
                        "where `source`['table'] = 'order_refund_info' " +
                        "and `op` = 'u' " +  // 操作类型为update
                        "and `before`['refund_status'] is not null " +  // 确保修改了refund_status字段
                        "and `after`['refund_status'] = '0705' " +  // 退单状态为退单完成
                        "and `after` is not null"
        );
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 4.3 筛选退款成功的订单表数据
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        "`after`['id'] as id," +  // 订单ID
                        "`after`['user_id'] as user_id," +  // 用户ID
                        "`after`['province_id'] as province_id " +  // 省份ID
                        "from ods_ecommerce_db " +
                        "where `source`['table'] = 'order_info' " +
                        "and `op` = 'u' " +  // 操作类型为update
                        "and `after`['order_status'] = '1006' " +  // 订单状态为退款完成
                        "and `after` is not null"
        );
        tEnv.createTemporaryView("order_info", orderInfo);

        // 5. 多表关联构建退款支付成功明细宽表
        Table resultTable = tEnv.sqlQuery(
                "select " +
                        "rp.id," +
                        "oi.user_id," +
                        "rp.order_id," +
                        "rp.sku_id," +
                        "oi.province_id," +
                        "rp.payment_type as payment_type_code," +  // 支付类型编码
                        "dic.dic_name as payment_type_name," +  // 支付类型名称
                        "date_format(to_timestamp_ltz(cast(rp.callback_time as bigint), 3), 'yyyy-MM-dd') as date_id," +
                        "rp.callback_time," +
                        "ori.refund_num," +
                        "rp.total_amount as refund_amount," +
                        "rp.ts " +
                        "from refund_payment rp " +  // 主表：退款支付表
                        "join order_refund_info ori " +  // 关联退单表
                        "on rp.order_id = ori.order_id and rp.sku_id = ori.sku_id " +
                        "join order_info oi " +  // 关联订单表
                        "on rp.order_id = oi.id " +
                        "join base_dic for system_time as of rp.pt as dic " +  // Lookup关联字典表
                        "on rp.payment_type = dic.dic_code"
        );
         resultTable.execute().print();  // 生产环境建议注释调试打印

        // 6. 创建Kafka Sink表
        String createSinkSql = "CREATE TABLE dwd_trade_refund_pay_suc_detail (\n" +
                "id STRING,\n" +
                "user_id STRING,\n" +
                "order_id STRING,\n" +
                "sku_id STRING,\n" +
                "province_id STRING,\n" +
                "payment_type_code STRING,\n" +
                "payment_type_name STRING,\n" +
                "date_id STRING,\n" +
                "callback_time STRING,\n" +
                "refund_num STRING,\n" +
                "refund_amount STRING,\n" +
                "ts BIGINT,\n" +
                "PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_REFUND_PAYMENT_SUCCESS_TOPIC);
        tEnv.executeSql(createSinkSql);

        // 7. 写入Kafka主题
        resultTable.executeInsert("dwd_trade_refund_pay_suc_detail");
    }
}
