package com.xcar.hbase.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/6/15
 * \* Time: 14:29
 * \* To change this template use File | Settings | File Templates.
 * \* Description: Hbase 配置类
 * \
 */

public class EasyConfiguration {

    private static String DEFAULT_PROPERTIES = "default.properties";
    private static int poo_size = EasyProperties.getInstance(DEFAULT_PROPERTIES).getInteger("easyhbase.client.ipc.pool.size");
    private static String hdfs_impl = EasyProperties.getInstance(DEFAULT_PROPERTIES).getString("easyhbase.fs.hdfs.impl");
    private static String EASYHBASE_PROPERTIES = EasyProperties.getInstance(DEFAULT_PROPERTIES).getString("easyhbase.default.meta.properties.filename");

    private static String zk_quorum = EasyProperties.getInstance(EASYHBASE_PROPERTIES).getString("easyhbase.general.zookeeper.quorum");
    private static String zk_znode = EasyProperties.getInstance(EASYHBASE_PROPERTIES).getString("easyhbase.general.zookeeper.znode.parent");
    private static String zk_port = EasyProperties.getInstance(EASYHBASE_PROPERTIES).getString("easyhbase.general.zookeeper.property.clientPort");
    private static long rpc_timeout = EasyProperties.getInstance(EASYHBASE_PROPERTIES).getLong("easyhbase.general.rpc.timeout");

    // todo 动态配置
    public static Configuration getConfiguration(){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zk_quorum);
        conf.set("zookeeper.znode.parent", zk_znode);
        conf.set("hbase.zookeeper.property.clientPort",zk_port);
        conf.setLong("hbase.rpc.timeout", rpc_timeout);
        conf.setInt("hbase.client.ipc.pool.size",poo_size);
        // No FileSystem for schema : hdfs
        conf.set("fs.hdfs.impl", hdfs_impl);
        return conf;
    }
}