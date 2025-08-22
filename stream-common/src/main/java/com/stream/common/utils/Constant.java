package com.stream.common.utils;





/**
 * 常量类（当前模块内，避免跨模块依赖）
 */
public final class Constant {
    private Constant() {}

    // Kafka相关
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "ods_page_log";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_cart_info";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL="dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS="dwd_trade_order_pay_suc_detail";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "cdh01:9092,cdh02:9092"; // Kafka集群地址

    // Doris相关
    public static final String DORIS_DATABASE = "retailersv1_dws"; // Doris数据库名
    public static final String DORIS_FE_NODES = "cdh01:8131"; // Doris FE节点
}