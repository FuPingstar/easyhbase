package com.xcar.hbase.api.dml;

import com.xcar.hbase.common.pojo.RowData;
import com.xcar.hbase.common.pojo.RowQuery;
import com.xcar.hbase.common.utils.EasyRowKeyUtil;
import com.xcar.hbase.common.utils.EasyTableUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/6/19
 * \* Time: 14:06
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class EasyDel {

    public EasyDel() {
    }

    /**
     * 删除任意行任意列的数据
     */
    public void delMultiRowsMultiColumns(Connection connection, String nameSpace, String tableName, String columnFamily,
                                         List<RowQuery> rowQueryList, int hashNums) throws IOException {
        final Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);
        ArrayList<Delete> delList = new ArrayList<>();
        for (RowQuery row : rowQueryList) {
            String originRowKey = row.getRowkey();
            String rowkey = EasyRowKeyUtil.hashRowkey(originRowKey, hashNums);

            Delete delete = new Delete(Bytes.toBytes(rowkey));
            List<String> columns = row.getColumns();
            for (String column : columns) {
                delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
            }
            delList.add(delete);
        }
        table.delete(delList);
        table.close();
    }

    /**
     * 删除一行数据
     */
    public void delOneRow(Connection connection, String nameSpace, String tableName,
                          String originRowKey, int hashNums) throws IOException {
        final Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);
        String rowkey = EasyRowKeyUtil.hashRowkey(originRowKey, hashNums);
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
        table.close();
    }

    /**
     * 删除多行数据
     */
    public void delMultiRows(Connection connection, String nameSpace, String tableName,
                          List<String> originRowKeys, int hashNums) throws IOException {
        final Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);
        ArrayList<Delete> delList = new ArrayList<>();
        for (String originRowKey : originRowKeys) {
            String rowkey = EasyRowKeyUtil.hashRowkey(originRowKey, hashNums);
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            delList.add(delete);
        }
        table.delete(delList);
        table.close();
    }
}