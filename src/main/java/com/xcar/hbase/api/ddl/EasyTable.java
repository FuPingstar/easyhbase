package com.xcar.hbase.api.ddl;

import com.xcar.hbase.api.dml.EasyGet;
import com.xcar.hbase.common.pojo.RowQuery;
import com.xcar.hbase.common.utils.EasyClientUtil;
import com.xcar.hbase.common.utils.EasyDateUtil;
import com.xcar.hbase.common.utils.EasyRowKeyUtil;
import com.xcar.hbase.common.utils.EasyTableUtil;
import com.xcar.hbase.core.EasyProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/6/19
 * \* Time: 14:06
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */

@Slf4j
public class EasyTable {

    // 元数据信息
    private static String DEFAULT_PROPERTIES = "default.properties";
    private static String metaTableName = EasyProperties.getInstance(DEFAULT_PROPERTIES).getString("easyhbase.default.meta.tablename");
    private static String metaNameSpace = EasyProperties.getInstance(DEFAULT_PROPERTIES).getString("easyhbase.default.meta.tablename.namespace");
    private static String metaColumnFamily = EasyProperties.getInstance(DEFAULT_PROPERTIES).getString("easyhbase.default.meta.tablename.columnFamily");

    public EasyTable() {
    }


    /**
     * 创建命名空间
     */
    public boolean createNamespace(Admin admin, String namespaceName) throws IOException {
        boolean existsnamespace = EasyTableUtil.existsNamespace(admin, namespaceName);
        if (existsnamespace) {
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespaceName).build();
            admin.createNamespace(namespaceDescriptor);
        }
        return Boolean.TRUE;
    }


    /**
     * 创建普通表
     */
    public boolean createTable(Connection connection, String namespaceName, String tableName, String columnFamily, int hashNums) throws IOException {

        Admin admin = connection.getAdmin();

        // 判断元数据表是否存在
        TableName tn_meta = EasyTableUtil.getTableName(metaNameSpace, metaTableName);
        boolean existsMeta = EasyTableUtil.existsTable(admin, tn_meta);
        if (!existsMeta) {
            throw new IOException("meta table '" + metaTableName + "' do not exits ! please first create the metaTable! ");
        }

        // 判断命名空间是否存在
        boolean exists = EasyTableUtil.existsNamespace(admin, namespaceName);
        if (!exists) {
            throw new IOException("NameSpace '" + namespaceName + "' do not exits ! please first create the namespace! ");
        }
        // 设置列族属性
        final TableName tn = EasyTableUtil.getTableName(namespaceName, tableName);
        HTableDescriptor hbaseTable = new HTableDescriptor(tn);
        final HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
        hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);// 指定压缩器，默认SNAPPY
        hColumnDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF); // 指定数据块编码，默认FAST_DIFF
        // 添加列族
        hbaseTable.addFamily(hColumnDescriptor);

        // 判断欲创建的表是否已存在
        final TableName ntm = EasyTableUtil.getTableName(namespaceName, tableName);
        if (admin.tableExists(ntm)) {
            throw new TableExistsException("Table " + tableName + " has exists !");
        }

        // 如果都不存在则创建表
        if (hashNums == 0) {
            admin.createTable(hbaseTable); // 不分区
        } else {
            byte[][] splits = new byte[hashNums][];
            // 对整数（十六进制）格式化
            for (int i = 0; i < hashNums; i++) {
                String format = String.format("%02x", i + 1);
                splits[i] = Bytes.toBytes(String.format(format));
            }
            admin.createTable(hbaseTable, splits);
        }
        log.info("success to create table '" + tableName + "'");
        admin.close();

        return true;
    }

    /**
     * 删除表
     */
    public boolean deleteTable(Connection connection, String namespaceName, String tableName) throws IOException {
        // 删除表
        Admin admin = connection.getAdmin();
        TableName tn = EasyTableUtil.getTableName(namespaceName, tableName);
        admin.disableTable(tn);
        admin.deleteTable(tn);
        admin.close();
        // 删除表元数据
        String rowkey = namespaceName+":"+tableName;
        deleteTableMeta(connection,rowkey);
        return true;
    }

    /**
     * 删除表元数据
     */
    public boolean deleteTableMeta(Connection connection, String rowkey) throws IOException {
        Table table = EasyTableUtil.getTable(connection, metaNameSpace, metaTableName);
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
        table.close();
        return true;
    }

    /**
     * 创建元数据表 easyhbase_metadata
     * rowkey 规则：namespaceName+":"+tableName
     */
    public boolean createMetaTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        // 判断元数据表是否存在
        TableName tn = EasyTableUtil.getTableName(metaNameSpace, metaTableName);
        boolean exists = EasyTableUtil.existsTable(admin, tn);
        if (!exists) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tn);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(metaColumnFamily);
            hColumnDescriptor.setInMemory(true);
            hTableDescriptor.addFamily(hColumnDescriptor);
            admin.createTable(hTableDescriptor);
            log.info("success to create metatable '" + metaTableName + "'");
        }else{
            throw new IOException("meta table " + metaTableName + " has exists !");
        }
        admin.close();
        return true;
    }

    /**
     * 更新表的元数据信息
     * rowkey 规则：namespaceName+":"+tableName
     */
    public boolean updateTableMetadata(Connection connection, String rowkey, Map<String, String> map) throws IOException {

        Table table = EasyTableUtil.getTable(connection, metaNameSpace, metaTableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        // jdk8 : forEach + Lambda表达式
        map.forEach((k,v)->{put.addColumn(metaColumnFamily.getBytes(), Bytes.toBytes(k), Bytes.toBytes(v));});
        table.put(put);
        log.info("success to create update table metadata of '" + rowkey + "'");
        table.close();
        return true;
    }


    /**
     * 随机灵活获取表的元数据信息
     */
    public Map<String, String> getTableMetadata(Connection connection, RowQuery rowQuery) throws IOException {
        EasyGet easyGet = new EasyGet();
        Table table = EasyTableUtil.getTable(connection, metaNameSpace, metaTableName);
        String rowkey = rowQuery.getRowkey();
        Get get = new Get(Bytes.toBytes(rowkey));
        get.setMaxVersions(1);
        List<String> columns = rowQuery.getColumns();
        for (String column : columns) {
            get.addColumn(Bytes.toBytes(metaColumnFamily), Bytes.toBytes(column));
        }
        final Map<String, String> rows = easyGet.getOneRow(table, get);
        table.close();
        // todo 表不存在如何快速判断
        if (rows.size() == 0) {
            throw new IOException("bad response ! Table '" + rowkey + "' maybe do not exists or table metadata is error !");
        }
        return rows;
    }

    /**
     * 指定rowkey获取表的全部元数据信息
     */
    public Map<String, String> getTableMetadata(Connection connection, String rowkey) throws IOException {
        EasyGet easyGet = new EasyGet();
        Table table = EasyTableUtil.getTable(connection, metaNameSpace, metaTableName);
        Get get = new Get(Bytes.toBytes(rowkey));
        get.setMaxVersions(1);
        final Map<String, String> rows = easyGet.getOneRow(table, get);
        table.close();
        // todo 表不存在如何快速判断
        if (rows.size() == 0) {
            throw new IOException("bad response ! Table '" + rowkey + "' maybe do not exists or table metadata is error !");
        }
        return rows;
    }

}