package com.xcar.hbase.api.dml;


import com.xcar.hbase.common.pojo.RowData;
import com.xcar.hbase.common.utils.EasyRowKeyUtil;
import com.xcar.hbase.common.utils.EasyTableUtil;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.*;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/6/19
 * \* Time: 14:06
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class EasyPut {

    public EasyPut() {
    }

    /**
     * K-V 插入单列值（同步写入）
     */
    public void putOneRowOneColumn(Connection connection, String nameSpace, String tableName,
                                   String columnFamily, String originRowKey, String columnName, String columnValue, int hashNums) throws IOException {
        final Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);
        String rowkey = EasyRowKeyUtil.hashRowkey(originRowKey, hashNums);
        Put put = new Put(Bytes.toBytes(rowkey));
        if (columnValue == null) {
            columnValue = "";
        }
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
        if (!put.isEmpty()) {
            table.put(put);
        }
        table.close();
    }

    /**
     * K-V 插入一行多列值（同步写入）
     */
    public void putOneRowMultiColumns(Connection connection, String nameSpace, String tableName,
                                      String columnFamily, RowData dataRow, int hashNums) throws IOException {
        final Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);

        String originRowKey = dataRow.getRowkey();
        String rowkey = EasyRowKeyUtil.hashRowkey(originRowKey, hashNums);
        Put put = new Put(Bytes.toBytes(rowkey));
        Map<String, String> keyValues = dataRow.getKeyValues();
        Iterator<Map.Entry<String, String>> iterator = keyValues.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> next = iterator.next();
            String nextKey = next.getKey();
            String nextValue = next.getValue();
            if (nextValue == null) {
                nextValue = "";
            }
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(nextKey), Bytes.toBytes(nextValue));
        }
        if (!put.isEmpty()) {
            table.put(put);
        }
        table.close();
    }

    /**
     * K-V 批量插入多行多列值（同步写入）
     */
    public void putMultiRowsMultiColumns(Connection connection, String nameSpace, String tableName, String columnFamily,
                                         List<RowData> dataRows, int hashNums) throws IOException {
        putMultiRowsMultiColumns(connection, nameSpace, tableName, columnFamily, dataRows, hashNums, false);
    }

    /**
     * K-V 批量插入多行多列值（同步写入）
     */
    public void putMultiRowsMultiColumns(Connection connection, String nameSpace, String tableName, String columnFamily,
                                         List<RowData> dataRows, int hashNums, boolean urlencode) throws IOException {
        val putList = new ArrayList<Put>();
        final Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);
        for (RowData row : dataRows) {
            String originRowKey = row.getRowkey();
            String rowkey = EasyRowKeyUtil.hashRowkey(originRowKey, hashNums);
            Put put = new Put(Bytes.toBytes(rowkey));
            Map<String, String> keyValues = row.getKeyValues();
            Iterator<Map.Entry<String, String>> iterator = keyValues.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> next = iterator.next();
                String nextKey = next.getKey();
                String nextValue = next.getValue();
                /*如果value进行了urlencode编码**/
                if (urlencode) {
                    nextValue = URLDecoder.decode(nextValue, "utf-8");
                }
                if (StringUtils.isBlank(nextValue) || ("null".equals(nextValue))) {
                    nextValue = "";  // 占用空间
                }
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(nextKey), Bytes.toBytes(nextValue));
            }
            putList.add(put);
        }
        // 批量插入
        if (!putList.isEmpty()) {
            table.put(putList);
        }
        table.close();
    }

}
