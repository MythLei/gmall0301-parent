package com.atguigu.gmall.realtime.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Felix
 * @date 2022/8/16
 * 操作phoenix的工具类
 */
public class PhoenixUtil {
    //执行sql语句的方法
    public static void executeSql(Connection conn,String sql){
        PreparedStatement ps = null;
        try {
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            ps.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            //释放资源
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
