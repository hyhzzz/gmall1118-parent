package com.atguigu.gmall.realtime.common;

/**
 * @author coderhyh
 * @create 2022-08-16 8:53
 * 实时数仓系统常量类
 */
public class GmallConfig {
    // Phoenix库名
    public static final String PHOENIX_SCHEMA = "GMALL1118_REALTIME";
    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    // Phoenix连接参数
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
}
