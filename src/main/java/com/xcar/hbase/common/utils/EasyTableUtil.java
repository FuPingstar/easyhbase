package com.xcar.hbase.common.utils;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/6/19
 * \* Time: 14:06
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class EasyTableUtil {

    /**
     * 返回带命名空间的表名
     */
    public static TableName getTableName(String nameSpace, String tableName) {
        return TableName.valueOf(nameSpace + ":" + tableName);
    }

    /**
     * 根据命名空间+表名拿到Table实例
     */
    public static Table getTable(Connection connection, String nameSpace, String tableName) throws IOException {
        TableName htableName = getTableName(nameSpace, tableName);
        return connection.getTable(htableName);
    }

    /**
     * 判断表是否存在
     */
    public static boolean existsTable(Admin admin, String nameSpace, String tableName) throws IOException {
        TableName htableName = getTableName(nameSpace, tableName);
        return admin.tableExists(htableName);
    }

    /**
     * 判断表是否存在
     */
    public static boolean existsTable(Admin admin, TableName tableName) throws IOException {
        return admin.tableExists(tableName);
    }

    /**
     * 判断命名空间是否存在
     */
    public static boolean existsNamespace(Admin admin, String namespaceName) {

        boolean boo = true;
        try {
            // 不报错即存在
            admin.getNamespaceDescriptor(namespaceName);
        } catch (IOException e) {
            boo=false;
        }
        return boo;
    }

}