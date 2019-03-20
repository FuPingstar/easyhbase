package com.xcar.hbase.client;

import com.xcar.hbase.api.ddl.EasyTable;
import com.xcar.hbase.api.dml.*;
import com.xcar.hbase.common.pojo.RowQuery;
import com.xcar.hbase.common.utils.EasyClientUtil;

import com.xcar.hbase.common.pojo.RowData;
import com.xcar.hbase.common.utils.EasyDateUtil;
import com.xcar.hbase.core.EasyProperties;
import lombok.Builder;
import lombok.Data;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/6/19
 * \* Time: 10:36
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
@Data
@Builder
public class EasyClient {

    private Connection connection;
    private String nameSpace;
    private String tableName;
    private String columnFamily;
    private int hashNums ;

    private static String DEFAULT_PROPERTIES = "default.properties";
    private static String EASYHBASE_PROPERTIES = EasyProperties.getInstance(DEFAULT_PROPERTIES).getString("easyhbase.default.meta.properties.filename");

    /**========================================================初始化=====================================================*/

    /**
     * 初始化连接
     * @throws IOException
     */
    public static EasyClient init() throws IOException {
        final Connection connection = EasyClientUtil.getConnection();
        EasyClient easyClient = EasyClient.builder().connection(connection).build();
        return easyClient;
    }

    /**
     * 指定表别名初始化连接
     * @throws IOException
     */
    public static EasyClient init(String tableAlias) throws IOException {
        final Connection connection = EasyClientUtil.getConnection();
        EasyClient easyClient = EasyClient.builder().connection(connection).build();
        easyClient.select(tableAlias);
        return easyClient;
    }

    /** 指定表别名切换表*/
    public void select(String tableAlias){
        String nameSpace = EasyProperties.getInstance(EASYHBASE_PROPERTIES).getString("easyhbase.table."+tableAlias+".namespace");
        String tableName = EasyProperties.getInstance(EASYHBASE_PROPERTIES).getString("easyhbase.table."+tableAlias+".name");
        String columnFamily = EasyProperties.getInstance(EASYHBASE_PROPERTIES).getString("easyhbase.table."+tableAlias+".cf");
        int hashNums = EasyProperties.getInstance(EASYHBASE_PROPERTIES).getInteger("easyhbase.table."+tableAlias+".hashNums");

        this.nameSpace=nameSpace;
        this.tableName=tableName;
        this.columnFamily=columnFamily;
        this.hashNums=hashNums;
    }

    /** 指定表别名切换表*/
    public void point(String nameSpace,String tableName,String columnFamily,int hashNums){

        this.nameSpace=nameSpace;
        this.tableName=tableName;
        this.columnFamily=columnFamily;
        this.hashNums=hashNums;
    }

    /** 指定表别名切换表*/
    public void point(String tableName){

        this.tableName=tableName;
    }

    /**========================================================Metadata=====================================================*/
    /**
     * 创建元数据表
     */
    public boolean createMetaTable() throws IOException {

        EasyTable easyTable = new EasyTable();
        final boolean isok = easyTable.createMetaTable(connection);
        return isok;
    }

    /**========================================================Table=====================================================*/

    /**
     * 创建数据表
     */
    public boolean createTable(String namespaceName,String tableName,String columnFamily,int hashNums) throws IOException {

        EasyTable easyTable = new EasyTable();
        final boolean isok = easyTable.createTable(connection, namespaceName, tableName, columnFamily, hashNums);
        if(isok){
            // 表创建成功更新元数据信息
            String rowkey = namespaceName+":"+tableName;
            Map<String, String> map = new HashMap<>();
            map.put("cf",columnFamily);
            map.put("hashNums",String.valueOf(hashNums));
            map.put("createDate",EasyDateUtil.getString(new Date(), "yyyy-MM-dd HH:mm:ss"));
            easyTable.updateTableMetadata(connection,rowkey,map);
        }
        return isok;
    }

    /**
     * 创建数据表
     */
    public boolean createTable(String tableName) throws IOException {

        EasyTable easyTable = new EasyTable();
        final boolean isok = easyTable.createTable(connection, nameSpace, tableName, columnFamily, hashNums);
        if(isok){
            // 表创建成功更新元数据信息
            String rowkey = nameSpace+":"+tableName;
            Map<String, String> map = new HashMap<>();
            map.put("cf",columnFamily);
            map.put("hashNums",String.valueOf(hashNums));
            map.put("createDate",EasyDateUtil.getString(new Date(), "yyyy-MM-dd HH:mm:ss"));
            easyTable.updateTableMetadata(connection,rowkey,map);
        }
        return isok;
    }

    /**========================================================GET=====================================================*/

    /**获取指定行指定列的值*/
    public String getOneRowOneColumn(String rowkey, String column) throws IOException {
        EasyGet easyGet = new EasyGet();
        return easyGet.getOneRowOneColumn(connection,nameSpace,tableName,rowkey,columnFamily,column,hashNums);
    }

    /**获取指定行多列的数据*/
    public Map<String, String> getOneRowMultiColumns(RowQuery rowQuery) throws IOException {
        EasyGet easyGet = new EasyGet();
        return easyGet.getOneRowMultiColumns(connection,nameSpace,tableName,columnFamily,rowQuery,hashNums);
    }

    /**获取指定行所有列的数据*/
    public Map<String, String> getOneRowAllColumns(String rowKey) throws IOException {
        EasyGet easyGet = new EasyGet();
        return easyGet.getOneCfAllColumns(connection,nameSpace,tableName,rowKey,columnFamily,hashNums);
    }

    /**获取任意多行任意列的数据*/
    public List<RowData> getMultiRowsMultiColumns(List<RowQuery> rowQueryList) throws IOException {
        EasyGet easyGet = new EasyGet();
        return easyGet.getMultiRowsMultiColumns(connection,nameSpace,tableName,columnFamily,rowQueryList,hashNums);
    }

    /**获取任意多行任意列的数据*/
    public List<RowData> getMultiRowsAllColumns(List<String> originRowKeys) throws IOException {
        EasyGet easyGet = new EasyGet();
        return easyGet.getMultiRowAllColumns(connection, nameSpace,tableName, originRowKeys,hashNums);
    }

    /**========================================================PUT=====================================================*/

    /**插入指定行指定列的数据*/
    public void putOneRowOneColumn(String rowKey, String columnName, String columnValue) throws IOException {
        EasyPut easyPut = new EasyPut();
        easyPut.putOneRowOneColumn(connection,nameSpace,tableName,columnFamily,rowKey,columnName,columnValue,hashNums);
    }

    /**插入指定行多列的数据*/
    public void putOneRowMultiColumn(RowData dataRow) throws IOException {
        EasyPut easyPut = new EasyPut();
        easyPut.putOneRowMultiColumns(connection,nameSpace,tableName,columnFamily,dataRow,hashNums);
    }

    /**插入任意多行任意多列的数据*/
    public void putMultiRowsMultiColumns(List<RowData> dataRows) throws IOException {
        EasyPut easyPut = new EasyPut();
        easyPut.putMultiRowsMultiColumns(connection,nameSpace,tableName,columnFamily,dataRows,hashNums);
    }

    /**=========================================================DELETE====================================================*/

    /**删除任意多行任意多列的数据*/
    public void delMultiRowsMultiColumns(List<RowQuery> rowQueryList) throws IOException {
        EasyDel easyDel = new EasyDel();
        easyDel.delMultiRowsMultiColumns(connection,nameSpace,tableName,columnFamily,rowQueryList,hashNums);
    }

    /**删除一整行数据*/
    public void delOneRow(String originRowKey) throws IOException {
        EasyDel easyDel = new EasyDel();
        easyDel.delOneRow(connection,nameSpace,tableName,originRowKey,hashNums);
    }

    /**同时删除多行数据*/
    public void delMultiRows(List<String> originRowKeys) throws IOException {
        EasyDel easyDel = new EasyDel();
        easyDel.delMultiRows(connection,nameSpace,tableName,originRowKeys,hashNums);
    }

    /**=========================================================Counter====================================================*/

    /**单列计数器*/
    public long incrOne(String originKey, String column) throws IOException {
        EasyIncr easyIncr = new EasyIncr();
        long count = easyIncr.IncrOne(connection, nameSpace, tableName, columnFamily, originKey, column, hashNums);
        return count;
    }

    /**=========================================================Scan====================================================*/

    /**单列计数器*/
    public List<RowData> getScanResult(Scan scan) throws IOException {
        EasyScan easyScan = new EasyScan();
        List<RowData> result = easyScan.getResult(connection, nameSpace, tableName, scan);
        return result;
    }


    /**=========================================================Close====================================================*/

    /**关闭连接*/
    public void close() throws IOException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}