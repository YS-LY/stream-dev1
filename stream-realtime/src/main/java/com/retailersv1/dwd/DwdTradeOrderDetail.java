package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// 交易域下单事务事实表
public class DwdTradeOrderDetail {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_ORDER_DETAIL_TOPIC = ConfigUtils.getString("kafka.dwd.trade.order.detail");

    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setStateBackend(new MemoryStateBackend());

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
        Table orderDetail = tEnv.sqlQuery(
                "select " +
                        "`after`['id'] as detail_id," +
                        "`after`['order_id'] as order_id," +
                        "`after`['sku_id'] as sku_id," +
                        "`after`['sku_name'] as sku_name," +
                        "`after`['create_time'] as create_time," +
                        "`after`['sku_num'] as sku_num," +
                        "`after`['order_price'] as order_price," +
                        "cast(cast(`after`['sku_num'] as decimal(16,2)) * " +
                        "cast(`after`['order_price'] as decimal(16,2)) as String) as split_original_amount," +
                        "`after`['split_total_amount'] as split_total_amount," +
                        "`after`['split_activity_amount'] as split_activity_amount," +
                        "`after`['split_coupon_amount'] as split_coupon_amount," +
                        "ts_ms as ts " +
                        "from ods_ecommerce_order " +
                        "where `source`['table'] = 'order_detail' " +
                        "and `after` is not null"
        );
        tEnv.createTemporaryView("order_detail", orderDetail);

        // 3.2 订单主表（复用ODS表）
        Table orderInfo = tEnv.sqlQuery("select\n" +
                "`after`['id'] as order_id,\n" +  // 订单主表的id别名为order_id
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

        // 4. 多表关联（修正TO_TIMESTAMP_LTZ参数错误）
        Table joinedTable = tEnv.sqlQuery(
                "select " +
                        "od.detail_id as id," +
                        "od.order_id as order_id," +
                        "oi.order_id as oi_order_id," +
                        "oi.user_id as user_id," +
                        "od.sku_id as sku_id," +
                        "od.sku_name as sku_name," +
                        "oi.province_id as province_id," +
                        "act.activity_id as activity_id," +
                        "cou.coupon_id as coupon_id," +
                        // 修正：TO_TIMESTAMP_LTZ需要2个参数（时间戳+精度）
                        "date_format(to_timestamp_ltz(cast(od.create_time as bigint), 3), 'yyyy-MM-dd') as date_id," +
                        "od.create_time as create_time," +
                        "od.sku_num as sku_num," +
                        "od.split_original_amount as split_original_amount," +
                        "od.split_activity_amount as split_activity_amount," +
                        "od.split_coupon_amount as split_coupon_amount," +
                        "od.split_total_amount as split_total_amount," +
                        "od.ts as ts " +
                        "from order_detail od " +
                        "join order_info oi on od.order_id = oi.order_id " +
                        "left join order_detail_activity act " +
                        "on od.detail_id = act.activity_detail_id " +
                        "left join order_detail_coupon cou " +
                        "on od.detail_id = cou.coupon_detail_id "
        );
         joinedTable.execute().print();  // 生产环境建议注释调试打印

        // 5. 创建Kafka Sink表
        String createSinkSql = "CREATE TABLE dwd_trade_order_detail (\n" +
                "id STRING,\n" +
                "order_id STRING,\n" +
                "oi_order_id STRING,\n" +
                "user_id STRING,\n" +
                "sku_id STRING,\n" +
                "sku_name STRING,\n" +
                "province_id STRING,\n" +
                "activity_id STRING,\n" +
                "coupon_id STRING,\n" +
                "date_id STRING,\n" +  // 与查询结果中的date_id对应
                "create_time STRING,\n" +
                "sku_num STRING,\n" +
                "split_original_amount STRING,\n" +
                "split_activity_amount STRING,\n" +
                "split_coupon_amount STRING,\n" +
                "split_total_amount STRING,\n" +
                "ts BIGINT,\n" +
                "PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(DWD_ORDER_DETAIL_TOPIC);
        tEnv.executeSql(createSinkSql);

        // 6. 写入Kafka
        //joinedTable.executeInsert("dwd_trade_order_detail");
    }
}
