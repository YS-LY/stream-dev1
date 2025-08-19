package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
//交易域取消订单事务事实表

import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderCancelDetail  {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_ORDER_DETAIL_TOPIC = ConfigUtils.getString("kafka.dwd.trade.order.cancel.detail");


    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 1. 设置TTL为15min + 5s
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15 * 60 + 5));

        // 2. 创建ODS层源表（从topic_db读取业务数据）
        tEnv.executeSql("CREATE TABLE ods_topic_db (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "  proc_time AS proctime()\n" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "dwd_trade_order_cancel_ods_consumer")
        );

        // 3. 筛选订单明细表数据（主表，insert操作，最细粒度）
        Table orderDetail = tEnv.sqlQuery("select\n" +
                "`after`['id'] as detail_id,\n" +
                "`after`['order_id'] as order_id,\n" +
                "`after`['sku_id'] as sku_id,\n" +
                "`after`['sku_name'] as sku_name,\n" +
                "`after`['sku_num'] as sku_num,\n" +
                "`after`['order_price'] as split_original_amount,\n" +
                "ts_ms as detail_ts\n" +
                "from ods_topic_db\n" +
                "where `source`['database'] = 'gmall'\n" +
                "and `source`['table'] = 'order_detail'\n" +
                "and `op` = 'insert'\n" +
                "and `after` is not null"
        );
        tEnv.createTemporaryView("order_detail", orderDetail);

        // 4. 筛选取消订单数据（order_info表，update操作，状态变更为1003）
        Table orderCancel = tEnv.sqlQuery("select\n" +
                "`after`['id'] as order_id,\n" +
                "`after`['user_id'] as user_id,\n" +
                "`after`['province_id'] as province_id,\n" +
                "`after`['operate_time'] as cancel_time,\n" +
                "ts_ms as cancel_ts\n" +
                "from ods_topic_db\n" +
                "where `source`['database'] = 'gmall'\n" +
                "and `source`['table'] = 'order_info'\n" +
                "and `op` = 'update'\n" +
                "and `before`['order_status'] is not null\n" +  // 确保修改了order_status字段
                "and `after`['order_status'] = '1003'\n" +     // 取消订单状态
                "and `after` is not null"
        );
        tEnv.createTemporaryView("order_cancel", orderCancel);

        // 5. 筛选订单明细活动关联表数据（insert操作）
        Table orderDetailActivity = tEnv.sqlQuery("select\n" +
                "`after`['order_id'] as activity_order_id,\n" +
                "`after`['order_detail_id'] as activity_detail_id,\n" +
                "`after`['activity_id'] as activity_id,\n" +
                "`after`['activity_rule_id'] as activity_rule_id\n" +
                "from ods_topic_db\n" +
                "where `source`['database'] = 'gmall'\n" +
                "and `source`['table'] = 'order_detail_activity'\n" +
                "and `op` = 'insert'\n" +
                "and `after` is not null"
        );
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // 6. 筛选订单明细优惠券关联表数据（insert操作）
        Table orderDetailCoupon = tEnv.sqlQuery("select\n" +
                "`after`['order_id'] as coupon_order_id,\n" +
                "`after`['order_detail_id'] as coupon_detail_id,\n" +
                "`after`['coupon_id'] as coupon_id\n" +
                "from ods_topic_db\n" +
                "where `source`['database'] = 'gmall'\n" +
                "and `source`['table'] = 'order_detail_coupon'\n" +
                "and `op` = 'insert'\n" +
                "and `after` is not null"
        );
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        // 7. 关联四张表获得取消订单宽表
        Table result = tEnv.sqlQuery("select\n" +
                "od.detail_id as id,\n" +
                "od.order_id,\n" +
                "oc.user_id,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "oc.province_id,\n" +
                "oda.activity_id,\n" +
                "oda.activity_rule_id,\n" +
                "odc.coupon_id,\n" +
                "date_format(oc.cancel_time, 'yyyy-MM-dd') as date_id,\n" +
                "oc.cancel_time,\n" +
                "od.sku_num,\n" +
                "od.split_original_amount,\n" +
                "cast(0 as string) as split_activity_amount,\n" +  // 取消订单暂不计算活动分摊，可根据实际业务调整
                "cast(0 as string) as split_coupon_amount,\n" +    // 取消订单暂不计算优惠券分摊，可根据实际业务调整
                "od.split_original_amount as split_total_amount,\n" +
                "oc.cancel_ts as ts\n" +
                "from order_detail od\n" +
                "join order_cancel oc on od.order_id = oc.order_id\n" +  // 内连接：只保留取消订单的明细
                "left join order_detail_activity oda \n" +
                "on od.order_id = oda.activity_order_id and od.detail_id = oda.activity_detail_id\n" +  // 左连接活动信息
                "left join order_detail_coupon odc \n" +
                "on od.order_id = odc.coupon_order_id and od.detail_id = odc.coupon_detail_id"    // 左连接优惠券信息
        );

        // 8. 创建Kafka Sink表
        String createSinkSql = "CREATE TABLE dwd_trade_order_cancel_detail (\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "date_id string,\n" +
                "cancel_time string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "ts bigint,\n" +
                "PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(DWD_ORDER_DETAIL_TOPIC);
        tEnv.executeSql(createSinkSql);

        // 9. 写入Kafka
        result.executeInsert("dwd_trade_order_cancel_detail");
    }
}
