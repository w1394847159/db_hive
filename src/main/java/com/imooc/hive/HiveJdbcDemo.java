package com.imooc.hive;

import java.sql.*;

/**
 * 使用JDBC操作Hive
 * 注意： 需要先启动 hiveserver2服务
 */
public class HiveJdbcDemo {

    public static void main(String[] args) throws SQLException {
        //jdbc的url
        String jdbcUrl = "jdbc:hive2://bigdata01:10000";
        //user为linux用户名，password可随意指定，不校验
        Connection conn = DriverManager.getConnection(jdbcUrl, "root", "root");

        Statement stmt = conn.createStatement();

        //指定查询的sql
        String sql = "select * from student";

        //执行sql
        ResultSet res = stmt.executeQuery(sql);

        //循环读取结果
        while (res.next()){
            System.out.println(res.getInt("id") + "\t" + res.getString("name"));

        }
    }

}
