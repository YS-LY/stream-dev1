package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// 交易域下单事务事实表


import java.time.Duration;

/**
 * @Package com.retailersv
 * @Author xiaoye
 * @Date 2025/8/18 19:39
 * @description: 下单事实表
 */
public class DwdTradeOrderDetail {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_TRADE_ORDER_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.detail");


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setStateBackend(new MemoryStateBackend());
        // 设置状态的保存时长
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        tableEnv.executeSql("CREATE TABLE ods_ecommerce_order (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "   proc_time AS proctime()" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "retailersv_dwd_order_detail"));

        Table orderDetail = tableEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['order_id'] order_id," +
                        "`after`['sku_id'] sku_id," +
                        "`after`['sku_name'] sku_name," +
                        "`after`['create_time'] create_time," +
                        "`after`['source_id'] source_id," +
                        "`after`['source_type'] source_type," +
                        "`after`['sku_num'] sku_num," +
                        "cast(cast(`after`['sku_num'] as decimal(16,2)) * " +
                        "   cast(`after`['order_price'] as decimal(16,2)) as String) split_original_amount," + // 分摊原始总金额
                        "`after`['split_total_amount'] split_total_amount," +  // 分摊总金额
                        "`after`['split_activity_amount'] split_activity_amount," + // 分摊活动金额
                        "`after`['split_coupon_amount'] split_coupon_amount," + // 分摊的优惠券金额
                        "ts_ms " +
                        "from ods_ecommerce_order " +
                        " where `source`['table'] ='order_detail'");
        tableEnv.createTemporaryView("order_detail", orderDetail);

//        orderDetail.execute().print();

        // 3. 过滤出 oder_info 数据: insert
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +
                        "`after`['user_id'] user_id," +
                        "`after`['province_id'] province_id " +
                        "from ods_ecommerce_order " +
                        "where `source`['table'] ='order_info' ");
        tableEnv.createTemporaryView("order_info", orderInfo);
//        orderInfo.execute().print();

        // 4. 过滤order_detail_activity 表: insert
        Table orderDetailActivity = tableEnv.sqlQuery(
                "select " +
                        "`after`['order_detail_id'] order_detail_id, " +
                        "`after`['activity_id'] activity_id, " +
                        "`after`['activity_rule_id'] activity_rule_id " +
                        "from ods_ecommerce_order " +
                        "where `source`['table'] ='order_detail_activity' ");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);
//        orderDetailActivity.execute().print();

        // 5. 过滤order_detail_coupon 表: insert
        Table orderDetailCoupon = tableEnv.sqlQuery(
                "select " +
                        "`after`['order_detail_id'] order_detail_id, " +
                        "`after`['coupon_id'] coupon_id " +
                        "from ods_ecommerce_order " +
                        "where `source`['table'] ='order_detail_coupon' ");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

//        orderDetailCoupon.execute().print();

        // 6. 四张表 join:
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id," +
                        "od.order_id," +
                        "oi.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "oi.province_id," +
                        "act.activity_id," +
                        "act.activity_rule_id," +
                        "cou.coupon_id," +
                        "date_format(FROM_UNIXTIME(cast(od.create_time as bigint) / 1000), 'yyyy-MM-dd') date_id," +
                        "od.create_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "od.ts_ms " +
                        "from order_detail od " +
                        "join order_info oi on od.order_id=oi.id " +
                        "left join order_detail_activity act " +
                        "on od.id=act.order_detail_id " +
                        "left join order_detail_coupon cou " +
                        "on od.id=cou.order_detail_id ");
//        result.execute().print();

        tableEnv.executeSql(
                "create table "+DWD_TRADE_ORDER_DETAIL+"(" +
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
                        "ts bigint," +
                        "primary key(id) not enforced " +
                        ")" + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_ORDER_DETAIL));



        result.executeInsert(DWD_TRADE_ORDER_DETAIL);


    }
}
