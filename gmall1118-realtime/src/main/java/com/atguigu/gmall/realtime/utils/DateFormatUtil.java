package com.atguigu.gmall.realtime.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author coderhyh
 * @create 2022-08-16 22:35
 * 日志转换的工具类
 * 注意：使用SimpleDateFormat进行日期转换  存在线程安全的问题
 * 以后在封装工具类的时候，使用jdk1.8以后的新的日期包下的相关类实现
 */
public class DateFormatUtil {
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static Long toTs(String dtStr, boolean isFull) {

        LocalDateTime localDateTime = null;
        if (!isFull) {
            dtStr = dtStr + " 00:00:00";
        }
        localDateTime = LocalDateTime.parse(dtStr, dtfFull);

        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }


    //把字符串类型的日期转换成毫秒数
    public static Long toTs(String dtStr) {
        return toTs(dtStr, false);
    }

    //把毫秒数转换成字符串 年月日
    public static String toDate(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    //把毫秒数转成字符串  年月日时分秒
    public static String toYmdHms(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfFull.format(localDateTime);
    }

    public static void main(String[] args) {
        System.out.println(toYmdHms(System.currentTimeMillis()));
    }
}

