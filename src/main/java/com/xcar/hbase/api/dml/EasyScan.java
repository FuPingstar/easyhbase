package com.xcar.hbase.api.dml;

import com.alibaba.fastjson.JSON;
import com.xcar.hbase.common.pojo.RowData;
import com.xcar.hbase.common.utils.EasyTableUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
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
 * \* Time: 14:06
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class EasyScan {

    private Scan scan;

    public EasyScan(){
        this.scan=new Scan();
    }

    public Scan getScan() {
        return scan;
    }

    public void setScan(Scan scan) {
        this.scan = scan;
    }

    public List<RowData> getResult(Connection connection, String nameSpace, String tableName, Scan scan) throws IOException {

        List<RowData> rows = new ArrayList<>();
        Table table = EasyTableUtil.getTable(connection, nameSpace, tableName);
        // 设置版本
        scan.setMaxVersions(1);
        // 设置列族和列

        // 设置startrow&endrow

        // 设置过滤器

        // 设置缓存

        // 设置时间范围
        scan.setColumnFamilyTimeRange(null,10,100);
        scan.setTimeRange(10,1000);

        // 设置最大返回数
        scan.setMaxResultSize(124);

        // 设置是否反转
        scan.setReversed(true);

//        if (args.length==4){
//            String rowkey = hashRowkey(args[3], 32);
//            scan.setStartRow(Bytes.toBytes(rowkey));
//        }

//        Date date = new Date();
//        SingleColumnValueFilter filter = new SingleColumnValueFilter(
//                Bytes.toBytes("f"),
//                Bytes.toBytes("day"),
//                CompareFilter.CompareOp.EQUAL,
//                new SubstringComparator(date.toInstant().toString().substring(0,10).replace("-","")));
//        //回补当天数据
//        scan.setFilter(filter);

        scan.setCaching(100);

        // 返回结果
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            Map<String, String> keyValues = new HashMap<>();
            String rowkey = Bytes.toString(result.getRow()).substring(3);
            for (KeyValue kv : result.raw()) {
                String qualifier = Bytes.toString(kv.getQualifier());
                String value = Bytes.toString(kv.getValue());

                keyValues.put(qualifier, value);
            }
            RowData rowData = RowData.builder().rowkey(rowkey).keyValues(keyValues).build();
            rows.add(rowData);
        }

        rows.clear();
        table.close();
        return rows;
    }

    public EasyScan test1(){
        scan.setStartRow(Bytes.toBytes("1"));
        return this;
    }

    public EasyScan test2(){
        scan.setStopRow(Bytes.toBytes("2"));
        return this;
    }


    public static void main(String[] args) {
        EasyScan easyScan = new EasyScan();
        System.out.println(easyScan.hashCode());
        System.out.println(easyScan.getScan().hashCode());
        easyScan = easyScan.test1().test2();
        System.out.println(easyScan.hashCode());
        System.out.println(easyScan.getScan().hashCode());
        System.out.println(Bytes.toString(easyScan.getScan().getStartRow()));
        System.out.println(Bytes.toString(easyScan.getScan().getStopRow()));

    }




}