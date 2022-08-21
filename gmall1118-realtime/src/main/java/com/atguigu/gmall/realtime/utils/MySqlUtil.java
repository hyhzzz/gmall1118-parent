package com.atguigu.gmall.realtime.utils;

/**
 * @author coderhyh
 * @create 2022-08-19 8:33
 * 操作MySQL的工具类
 */
public class MySqlUtil {
    //从字典表中读取数据创建动态表的建表语句
    public static String getBaseDicDDL() {
        return "CREATE TABLE base_dic (\n" +
                "    `dic_code` string,\n" +
                "    `dic_name` string,\n" +
                "    `parent_code` string, \n" +
                "    `create_time` TIMESTAMP,\n" +
                "    `operate_time` TIMESTAMP,\n" +
                "    PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")" + getJdbcDDL("base_dic");
    }

    //获取JDBC连接器相关连接属性
    public static String getJdbcDDL(String tableName) {
        return "WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop102:3306/gmall-1118',\n" +
                "   'table-name' = '" + tableName + "',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'lookup.cache.max-rows' = '200',\n" +
                "   'lookup.cache.ttl' = '1 hour'\n" +
                ")";
    }
}
