package com.lyf.streaming.qz.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Project: edu_online
 * Create by lyf3312 on 20/04/07 18:42
 * Version: 1.0
 * 德鲁伊连接池工具类
 */
public class DbUtil {
    private static DataSource ds = null;

    //类加载时初始化ds
    static {
        try {
            Properties props = new Properties();
            props.setProperty("url", ConfigurationManager.getProperty("jdbc.url"));
            props.setProperty("username", ConfigurationManager.getProperty("jdbc.user"));
            props.setProperty("password", ConfigurationManager.getProperty("jdbc.password"));
            props.setProperty("initialSize", "5"); //初始化大小
            props.setProperty("maxActive", "10"); //最大连接
            props.setProperty("minIdle", "5");  //最小连接
            props.setProperty("maxWait", "60000"); //等待时长
            props.setProperty("timeBetweenEvictionRunsMillis", "2000");//配置多久进行一次检测,检测需要关闭的连接 单位毫秒
            props.setProperty("minEvictableIdleTimeMillis", "600000");//配置连接在连接池中最小生存时间 单位毫秒
            props.setProperty("maxEvictableIdleTimeMillis", "900000"); //配置连接在连接池中最大生存时间 单位毫秒
            props.setProperty("validationQuery", "select 1");
            props.setProperty("testWhileIdle", "true");
            props.setProperty("testOnBorrow", "false");
            props.setProperty("testOnReturn", "false");
            props.setProperty("keepAlive", "true");
            props.setProperty("phyMaxUseCount", "100000");
            ds = DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //从池中获取连接
    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    // 提供关闭资源的方法【connection是归还到连接池】
    public static void closeResource(ResultSet resultSet, PreparedStatement preparedStatement,
                                     Connection connection) {
        // 关闭结果集
        // ctrl+alt+m 将java语句抽取成方法
        closeResultSet(resultSet);
        // 关闭语句执行者
        closePrepareStatement(preparedStatement);
        // 关闭连接
        closeConnection(connection);
    }

    private static void closePrepareStatement(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    private static void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    private static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
