package com.xcar.hbase.api.dml;

import com.xcar.hbase.common.pojo.RowQuery;
import com.xcar.hbase.common.pojo.RowData;
import com.xcar.hbase.common.utils.EasyRowKeyUtil;
import com.xcar.hbase.common.utils.EasyTableUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/6/19
 * \* Time: 14:05
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class EasyGet {

    private static String pvCounter = "readcount";

    public EasyGet() {
    }

    // todo rowkey 非空校验

    /**
     * 获取任意行任意列的数据
     */
    public List<RowData> getMultiRowsMultiColumns(Connection connection, String nameSpace, String tableName, String columnFamily,
                                                  List<RowQuery> rowQueryList, int hashNums) throws IOException {
        final Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);
        ArrayList<Get> getList = new ArrayList<>();
        for (RowQuery row : rowQueryList) {
            String originRowKey = row.getRowkey();
            String rowkey = EasyRowKeyUtil.hashRowkey(originRowKey, hashNums);
            Get get = new Get(Bytes.toBytes(rowkey));
            get.setMaxVersions(1);
            List<String> columns = row.getColumns();
            for (String column : columns) {
                get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
            }
            getList.add(get);
        }
        final List<RowData> rows = getRows(table, getList, hashNums);
        table.close();
//        if (rows.size() == 0) {
//            throw new IOException("Rowkeys do not exists in Table '" + tableName + "' !");
//        }
        return rows;
    }

    /**
     * 获取指定行多列的数据
     */
    public Map<String, String> getOneRowMultiColumns(Connection connection, String nameSpace, String tableName, String columnFamily,
                                                     RowQuery rowQuery, int hashNums) throws IOException {
        final Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);
        String originRowKey = rowQuery.getRowkey();
        String rowkey = EasyRowKeyUtil.hashRowkey(originRowKey, hashNums);
        Get get = new Get(Bytes.toBytes(rowkey));
        get.setMaxVersions(1);
        List<String> columns = rowQuery.getColumns();
        for (String column : columns) {
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        }
        final Map<String, String> rows = getOneRow(table, get);
        table.close();
//        if (rows.size() == 0) {
//            throw new IOException("Rowkey '" + originRowKey + "' do not exists in Table '" + tableName + "' !");
//        }
        return rows;
    }

    /**
     * 获取单行所有列族
     */
    public List<RowData> getMultiRowAllColumns(Connection connection, String nameSpace, String tableName,
                                                   List<String> originRowKeys, int hashNums) throws IOException {
        final Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);
        ArrayList<Get> getList = new ArrayList<>();
        for (String originRowKey : originRowKeys) {
            String rowkey = EasyRowKeyUtil.hashRowkey(originRowKey, hashNums);
            Get get = new Get(Bytes.toBytes(rowkey));
            get.setMaxVersions(1);
            getList.add(get);
        }
        final List<RowData> rows = getRows(table, getList, hashNums);
        table.close();
//        if (rows.size() == 0) {
//            throw new IOException("Rowkeys do not exists in Table '" + tableName + "' !");
//        }
        return rows;
    }

    /**
     * 获取单行所有列族
     */
    public Map<String, String> getOneRowAllColumns(Connection connection, String nameSpace, String tableName,
                                                   String originRowKey, int hashNums) throws IOException {
        final Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);
        String rowkey = EasyRowKeyUtil.hashRowkey(originRowKey, hashNums);
        Get get = new Get(Bytes.toBytes(rowkey));
        get.setMaxVersions(1);
        final Map<String, String> oneRow = getOneRow(table, get);
        table.close(); // 关闭连接
        if (oneRow.size() == 0) {
            throw new IOException("Rowkey '" + originRowKey + "' do not exists in Table '" + tableName + "' !");
        }
        return oneRow;
    }

    /**
     * 获取单行指定列族所有列
     */
    public Map<String, String> getOneCfAllColumns(Connection connection, String nameSpace, String tableName,
                                                  String originRowKey, String columnfamily, int hashNums) throws IOException {
        final Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);
        String rowkey = EasyRowKeyUtil.hashRowkey(originRowKey, hashNums);
        Get get = new Get(Bytes.toBytes(rowkey));
        get.setMaxVersions(1);
        get.addFamily(Bytes.toBytes(columnfamily));  // 指定列族
        final Map<String, String> onecf = getOneRow(table, get);
        table.close(); // 关闭连接
        if (onecf.size() == 0) {
            throw new IOException("Rowkey '" + originRowKey + "' do not exists in Table '" + tableName + "' !");
        }
        return onecf;
    }

    /**
     * 获取单列
     */
    public String getOneRowOneColumn(Connection connection, String nameSpace, String tableName, String originRowKey,
                                     String columnfamily, String column, int hashNums) throws IOException {
        final Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);
        String rowkey = EasyRowKeyUtil.hashRowkey(originRowKey, hashNums);
        final byte[] oneColumn = getOneColumn(table, rowkey, columnfamily, column, originRowKey);
        String value = Bytes.toString(oneColumn);
        // 过滤操作
        value = valueFilter(column,value);
        table.close(); // 关闭连接
        return value;
    }

    /**
     * 获取单列,返回字节数组
     */
    public byte[] getOneRowOneColumnToBytes(Connection connection, String nameSpace, String tableName,
                                            String originRowKey, String columnfamily, String column, int hashNums) throws IOException {
        final Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);
        String rowkey = EasyRowKeyUtil.hashRowkey(originRowKey, hashNums);
        final byte[] oneColumn = getOneColumn(table, rowkey, columnfamily, column, originRowKey);
        table.close(); // 关闭连接
        return oneColumn;
    }

    /**********************************************************************************************************************************
     * 封装方法：返回单列值
     */
    public byte[] getOneColumn(Table table, String rowkey, String columnfamily, String column, String originRowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowkey));
        get.setMaxVersions(1);
        get.addColumn(Bytes.toBytes(columnfamily), Bytes.toBytes(column));
        final Result result = table.get(get);
        byte[] value = result.getValue(Bytes.toBytes(columnfamily), Bytes.toBytes(column));
        return value;
    }


    /**
     * 封装方法：返回多列值
     */
    public Map<String, String> getOneRow(Table table, Get get) throws IOException {
        Map<String, String> keyValues = new HashMap<>();
        final Result result = table.get(get);
        for (KeyValue kv : result.raw()) {
            String qualifier = Bytes.toString(kv.getQualifier());
            String value = Bytes.toString(kv.getValue());
            // 过滤操作
            value = valueFilter(qualifier,value);
            keyValues.put(qualifier, value);
        }
        return keyValues;
    }

    /**
     * 封装方法：返回多行多列值
     */
    public List<RowData> getRows(Table table, List<Get> getList, int hashNums) throws IOException {
        List<RowData> rowDatas = new ArrayList<>();
        Result[] results = table.get(getList);
        for (Result result : results) {
            String rowkey = Bytes.toString(result.getRow());
            if (rowkey == null) {
                continue;
            }
            if (hashNums != 0 && rowkey != null) {
                rowkey = rowkey.substring(3);
            }
            HashMap<String, String> keyValues = new HashMap<>();
            for (KeyValue kv : result.raw()) {
                String qualifier = Bytes.toString(kv.getQualifier());
                String value = Bytes.toString(kv.getValue());
                // 过滤操作
                value = valueFilter(qualifier, value);
                keyValues.put(qualifier, value);
            }
            RowData rowData = RowData.builder().rowkey(rowkey).keyValues(keyValues).build();
            rowDatas.add(rowData);
        }
        return rowDatas;
    }

    /** 附加过滤操作*/
    public String  valueFilter(String qualifier,String value){
        if("null".equals(value)){
            value="";
        }
        if(pvCounter.equals(qualifier)){
            if(StringUtils.isEmpty(value)){
                return "0";
            }else {
                value = String.valueOf(Bytes.toLong(Bytes.toBytes(value)));
            }
        }
        return value;
    }
}