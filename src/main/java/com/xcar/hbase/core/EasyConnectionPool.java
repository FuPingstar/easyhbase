package com.xcar.hbase.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/6/15
 * \* Time: 9:34
 * \* To change this template use File | Settings | File Templates.
 * \* Description: Hbase 连接管理
 * \
 */
@Slf4j
public class EasyConnectionPool {
    private Connection connection;
    private static EasyConnectionPool easyConnectionPool = null;

    private static String DEFAULT_PROPERTIES = "default.properties";
    private static String SUPER_USER = EasyProperties.getInstance(DEFAULT_PROPERTIES).getString("easyhbase.default.superuser");

    private EasyConnectionPool() {
    }

    // 单例模式
    public synchronized static EasyConnectionPool getPool() {
        if (easyConnectionPool == null) {
            easyConnectionPool = new EasyConnectionPool();
        }
        return easyConnectionPool;
    }

    // 获取Hbase连接
    public Connection connect(Configuration conf) throws IOException {
        if (connection == null || connection.isClosed()) {
            // 默认指定hbase用户访问hbase集群
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation romoteUser = UserGroupInformation.createRemoteUser(SUPER_USER);
            UserGroupInformation.setLoginUser(romoteUser);
            connection = ConnectionFactory.createConnection(conf);
        }
        return connection;
    }


    // 关闭Hbase连接
    public void close(Connection connection) throws IOException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}