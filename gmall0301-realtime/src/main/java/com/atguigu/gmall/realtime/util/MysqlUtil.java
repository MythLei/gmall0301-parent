package com.atguigu.gmall.realtime.util;

/**
 * @author Felix
 * @date 2022/8/21
 * 操作MySQL的工具类
 */
public class MysqlUtil {
    public static String getBaseDicLookUpDDL() {
        return "CREATE TABLE base_dic (\n" +
            "  dic_code string,\n" +
            "  dic_name STRING,\n" +
            "  PRIMARY KEY (dic_code) NOT ENFORCED\n" +
            ") " + mysqlLookUpTableDDL("base_dic");
    }

    public static String mysqlLookUpTableDDL(String tableName) {
        return " WITH (\n" +
            "   'connector' = 'jdbc',\n" +
            "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
            "   'url' = 'jdbc:mysql://hadoop202:3306/gmall0301',\n" +
            "   'table-name' = '" + tableName + "',\n" +
            "   'lookup.cache.max-rows' = '500',\n" +
            "   'lookup.cache.ttl' = '1 hour',\n" +
            "   'username' = 'root',\n" +
            "   'password' = '123456'\n" +
            ")";
    }
}
