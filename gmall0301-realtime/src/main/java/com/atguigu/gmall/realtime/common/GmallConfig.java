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
}
