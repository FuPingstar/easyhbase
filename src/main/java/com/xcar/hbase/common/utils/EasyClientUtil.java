package com.xcar.hbase.common.utils;

import com.xcar.hbase.client.EasyClient;
import com.xcar.hbase.core.EasyConfiguration;
import com.xcar.hbase.core.EasyConnectionPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import java.io.IOException;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/8/16
 * \* Time: 11:20
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class EasyClientUtil {

    /**获取配置类*/
    public static Configuration getConfiguration(){
        return EasyConfiguration.getConfiguration();
    }

    /**获取连接*/
    public static Connection getConnection(Configuration configuration) throws IOException {
        return EasyConnectionPool.getPool().connect(configuration);
    }

    /**获取连接*/
    public static Connection getConnection() throws IOException {
        final Configuration configuration = EasyConfiguration.getConfiguration();
        return EasyConnectionPool.getPool().connect(configuration);
    }
}