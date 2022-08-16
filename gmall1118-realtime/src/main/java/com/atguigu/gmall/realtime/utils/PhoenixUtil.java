package com.atguigu.gmall.realtime.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author coderhyh
 * @create 2022-08-16 13:26
 * 操作phoenix的工具类
 */
public class PhoenixUtil {

    //向phoenix表中插入数据
    public static void executeSQL(String upsertSQL, Connection conn) {

        PreparedStatement ps = null;

        try {
            //获取数据库操作对象
            ps = conn.prepareStatement(upsertSQL);

            //执行SQL语句
            ps.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("执行操作phoenix语句发生了异常");
        } finally {
            //释放资源
            close(ps, conn);
        }
    }
    private static void close(PreparedStatement ps, Connection conn) {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}