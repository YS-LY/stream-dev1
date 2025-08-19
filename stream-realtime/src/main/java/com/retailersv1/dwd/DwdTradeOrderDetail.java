package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//交易域下单事务事实表
public class DwdTradeOrderDetail {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_ORDER_DETAIL_TOPIC = ConfigUtils.getString("kafka.dwd.order.detail");

    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2. 创建ODS层源表（复用同一Kafka主题，通过source.table过滤）
        tEnv.executeSql("CREATE TABLE ods_ecommerce_order (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "  proc_time AS proctime()\n" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "retailersv_ods_ecommerce_order")
        );

        // 3. 提取各表数据并创建临时视图
        // 3.1 订单详情表
        Table orderDetail = tEnv.sqlQuery("select\n" +
                "`after`['id'] as detail_id,\n" +
                "`after`['order_id'] as order_id,\n" +
                "`after`['sku_id'] as sku_id,\n" +
                "`after`['sku_num'] as sku_num,\n" +
                "`after`['sku_name'] as sku_name,\n" +
                "`after`['order_price'] as order_price,\n" +
                "ts_ms as detail_ts\n" +
                "from ods_ecommerce_order\n" +
                "where `source`['table'] = 'order_detail' and `after` is not null"
        );
        tEnv.createTemporaryView("order_detail", orderDetail);

        // 3.2 订单主表（复用ODS表）
        Table orderInfo = tEnv.sqlQuery("select\n" +
                "`after`['id'] as order_id,\n" +
                "`after`['user_id'] as user_id,\n" +
                "`after`['order_status'] as order_status,\n" +
                "`after`['create_time'] as create_time,\n" +
                "`after`['province_id'] as province_id\n" +
                "from ods_ecommerce_order\n" +
                "where `source`['table'] = 'order_info' and `after` is not null"
        );
        tEnv.createTemporaryView("order_info", orderInfo);

        // 3.3 订单详情活动表（复用ODS表）
        Table orderDetailActivity = tEnv.sqlQuery("select\n" +
                "`after`['order_id'] as activity_order_id,\n" +
                "`after`['order_detail_id'] as activity_detail_id,\n" +
                "`after`['activity_id'] as activity_id\n" +
                "from ods_ecommerce_order\n" +
                "where `source`['table'] = 'order_detail_activity' and `after` is not null"
        );
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // 3.4 订单详情优惠券表（复用ODS表）
        Table orderDetailCoupon = tEnv.sqlQuery("select\n" +
                "`after`['order_id'] as coupon_order_id,\n" +
                "`after`['order_detail_id'] as coupon_detail_id,\n" +
                "`after`['coupon_id'] as coupon_id\n" +
                "from ods_ecommerce_order\n" +
                "where `source`['table'] = 'order_detail_coupon' and `after` is not null"
        );
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        // 4. 多表关联
        Table joinedTable = tEnv.sqlQuery("select\n" +
                "od.detail_id,\n" +
                "od.order_id,\n" +
                "od.sku_id,\n" +
                "oi.user_id,\n" +
                "oda.activity_id,\n" +
                "odc.coupon_id,\n" +
                "od.detail_ts as ts\n" +
                "from order_detail od\n" +
                "left join order_info oi on od.order_id = oi.order_id\n" +
                "left join order_detail_activity oda on od.order_id = oda.activity_order_id and od.detail_id = oda.activity_detail_id\n" +
                "left join order_detail_coupon odc on od.order_id = odc.coupon_order_id and od.detail_id = odc.coupon_detail_id"
        );

        // 5. 创建Kafka Sink表（确保配置有效）
        String createSinkSql = "CREATE TABLE dwd_trade_order_detail (\n" +
                "detail_id STRING,\n" +
                "order_id STRING,\n" +
                "sku_id STRING,\n" +
                "user_id STRING,\n" +
                "activity_id STRING,\n" +
                "coupon_id STRING,\n" +
                "ts BIGINT,\n" +
                "PRIMARY KEY (detail_id) NOT ENFORCED\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(DWD_ORDER_DETAIL_TOPIC);
        tEnv.executeSql(createSinkSql); // 执行建表语句

        // 6. 写入Kafka（关键：触发Sink算子）
        // 注意：executeInsert会生成Sink算子，必须执行且不被注释
        joinedTable.executeInsert("dwd_trade_order_detail");

//        // 7. 执行作业（此时拓扑中已有Sink算子，可正常执行）
//        env.execute("DWD Order Detail to Kafka");
    }
}