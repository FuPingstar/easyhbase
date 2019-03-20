package com.xcar.hbase.api.dml;

import com.xcar.hbase.common.pojo.RowData;
import com.xcar.hbase.common.utils.EasyRowKeyUtil;
import com.xcar.hbase.common.utils.EasyTableUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * 计数器相关
 */
public class EasyIncr {


    /**
     * 步长为1的单列计数器：插入
     */
    public long IncrOne(Connection connection, String nameSpace, String tableName,
                        String columnFamily, String originKey, String column, int hashNums) throws IOException {

        final Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);

        String rowkey = EasyRowKeyUtil.hashRowkey(originKey, hashNums);
        long count = table.incrementColumnValue(Bytes.toBytes(rowkey), Bytes.toBytes(columnFamily), Bytes.toBytes(column), 1L);

        table.close();
        return count;
    }

    /**
     * 步长为1的多列计数器：插入
     */
    public long IncrOne2(Connection connection, String nameSpace, String tableName,
                        String columnFamily, String originKey, String column, int hashNums) throws IOException {
//
        final Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);

        Increment increment = new Increment(Bytes.toBytes("row-rc"));
        increment.addColumn(Bytes.toBytes("test"), Bytes.toBytes("incr"), 6);
        increment.addColumn(Bytes.toBytes("test"), Bytes.toBytes("incr2"), 10);
        Result result = table.increment(increment);

        KeyValue[] kvs=result.raw();
        for(KeyValue kv:kvs){
            System.out.println(kv.toString());
            System.out.println(Bytes.toString(kv.getValue()));
        }

//
        String rowkey = EasyRowKeyUtil.hashRowkey(originKey, hashNums);
        long count = table.incrementColumnValue(Bytes.toBytes(rowkey), Bytes.toBytes(columnFamily), Bytes.toBytes(column), 1L);

        table.close();
        return 0L;
    }

}
