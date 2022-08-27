package com.atguigu.gmall.realtime.util;

/**
 * @author Felix
 * @date 2022/8/27
 * Desc: FlinkSQL时间类型TimestampLtz3比较工具类
 */
public class TimestampLtz3CompareUtil {
    public static int compare(String timestamp1, String timestamp2) {
        // 数据格式 2022-04-01 10:20:47.302Z
        // 1. 去除末尾的时区标志，'Z' 表示 0 时区
        String cleanedTime1 = timestamp1.substring(0, timestamp1.length() - 1);
        String cleanedTime2 = timestamp2.substring(0, timestamp2.length() - 1);
        // 2. 比较时间
        return cleanedTime1.compareTo(cleanedTime2);
    }

    public static void main(String[] args) {
        System.out.println(compare("2022-04-01 11:10:55.049Z",
            "2022-04-01 11:10:55.04Z"));
    }
}

