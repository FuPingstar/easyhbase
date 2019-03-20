package com.xcar.hbase;

import com.xcar.hbase.client.EasyClient;
import com.xcar.hbase.common.pojo.RowData;
import com.xcar.hbase.common.pojo.RowQuery;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/8/16
 * \* Time: 17:53
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class EasyClientTestGet {

    /**
     * 0.初始化
     */
    public EasyClient init() {
        EasyClient client = null;
        try {
            client = EasyClient.init("t1");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return client;
    }
    
    /**
     * 1.获取单行单列值测试
     */
    @Test
    public void test_getOneRowOneColumn() {
        EasyClient easyClient = init();
        // String rowkey = .....   自行设计rowkey
        try {
            final long t0 = System.currentTimeMillis();
//            for (int i = 1000; i < 2000; i++) {
                final long t1 = System.currentTimeMillis();
                final String value = easyClient.getOneRowOneColumn("row3", "column1");
                final long t2 = System.currentTimeMillis();
                System.out.println("----"+(t2-t1));
//            }
            final long t3 = System.currentTimeMillis();
            System.out.println(t3-t0);
            easyClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 2.获取单行多列值测试
     */
    @Test
    public void test_getOneRowMultiColumns() {
        // String rowkey = .....   自行设计rowkey
        EasyClient easyClient = init();
        // 构造数据
        final ArrayList<String> list = new ArrayList<>();
        list.add("name");
        list.add("sex");
        final RowQuery rowQuery = RowQuery.builder().rowkey("rowkey_2").columns(list).build();
        try {
            final Map<String, String> oneRowMultiColumns = easyClient.getOneRowMultiColumns(rowQuery);
            final Iterator<Map.Entry<String, String>> iterator = oneRowMultiColumns.entrySet().iterator();
            while (iterator.hasNext()){
                final Map.Entry<String, String> next = iterator.next();
                final String nextKey = next.getKey();
                final String nextValue = next.getValue();
                System.out.println(nextKey+" : "+nextValue);
            }
            easyClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 3.获取单行所有列数据测试
     */
    @Test
    public void test_getOneRowAllColumns() {
        // String rowkey = .....   自行设计rowkey
        EasyClient easyClient = init();
        try {
            final Map<String, String> oneRowAllColumns = easyClient.getOneRowAllColumns("rowkey_4");
            final Iterator<Map.Entry<String, String>> iterator = oneRowAllColumns.entrySet().iterator();
            while (iterator.hasNext()){
                final Map.Entry<String, String> next = iterator.next();
                final String nextKey = next.getKey();
                final String nextValue = next.getValue();
                System.out.println(nextKey+" : "+nextValue);
            }
            easyClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 4.获取任意多行多列值测试
     */
    @Test
    public void test_getMultiRowsMultiColumns() {
        // String rowkey = .....   自行设计rowkey
        EasyClient easyClient = init();
        // 构造数据
        final ArrayList<RowQuery> arrayList = new ArrayList<>();

        final ArrayList<String> list = new ArrayList<>();
        list.add("name");
        list.add("sex");
        final RowQuery rowQuery = RowQuery.builder().rowkey("rowkey_3").columns(list).build();
        arrayList.add(rowQuery);

        final ArrayList<String> list2 = new ArrayList<>();
        list2.add("name");
        list2.add("age");
        final RowQuery rowQuery2 = RowQuery.builder().rowkey("rowkey_4").columns(list2).build();
        arrayList.add(rowQuery2);

        try {
            final List<RowData> multiRowsMultiColumns = easyClient.getMultiRowsMultiColumns(arrayList);
            for (RowData rowData:multiRowsMultiColumns){
                final String rowKey = rowData.getRowkey();
                final Map<String, String> keyValues = rowData.getKeyValues();
                final Iterator<Map.Entry<String, String>> iterator = keyValues.entrySet().iterator();
                while (iterator.hasNext()){
                    final Map.Entry<String, String> next = iterator.next();
                    final String nextKey = next.getKey();
                    final String nextValue = next.getValue();
                    System.out.println(rowKey+" : "+nextKey+" : "+nextValue);
                }
            }
            easyClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}