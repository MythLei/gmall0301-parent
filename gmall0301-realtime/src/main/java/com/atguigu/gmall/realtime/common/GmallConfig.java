package com.atguigu.gmall.realtime.common;

/**
 * @author Felix
 * @date 2022/8/16
 * 数仓系统常量类
 */
public class GmallConfig {
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop202,hadoop203,hadoop204:2181";
    public static final String PHOENIX_SCHEMA = "GMALL0301_REALTIME";
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop203:8123/default";
}
