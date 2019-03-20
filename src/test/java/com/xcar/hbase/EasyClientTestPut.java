package com.xcar.hbase;

import com.xcar.hbase.client.EasyClient;
import com.xcar.hbase.common.pojo.RowData;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/8/16
 * \* Time: 17:53
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class EasyClientTestPut {

    /**
     * 0.初始化
     */
    public EasyClient init() {
        EasyClient client = null;
        try {
            client = EasyClient.init("t2");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return client;
    }

    /**
     * 1.切换表测试
     */
    @Test
    public void test_select() {
        EasyClient easyClient = init();
        easyClient.select("t2");
        System.out.println(easyClient.getTableName());
        try {
            easyClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 2.插入单行单列值测试
     */
    @Test
    public void test_putOneRowOneColumn() {
        EasyClient easyClient = init();
        // String rowkey = .....   自行设计rowkey
        try {
            final long t0 = System.currentTimeMillis();
            for (int i = 0; i < 10000; i++) {
                final long t1 = System.currentTimeMillis();
                easyClient.putOneRowOneColumn("rowkey_" + i, "name", "zhou");
                final long t2 = System.currentTimeMillis();
                System.out.println(t2 - t1);
            }
            final long t3 = System.currentTimeMillis();
            System.out.println(t3 - t0);
            easyClient.close();
        } catch (RetriesExhaustedWithDetailsException e) {
            System.out.println(123);
            final List<Throwable> causes = e.getCauses();
            for (Throwable cause : causes) {
                if(cause instanceof NoSuchColumnFamilyException){
                    System.out.println(cause.getClass()+"no such columnFamily");
                }
            }
        } catch (NoSuchColumnFamilyException e) { // 捕捉不到
            System.out.println(456);
        } catch (IOException e) {
            System.out.println(789);
        }
    }

    /**
     * 3.插入单行多列值测试
     */
    @Test
    public void test_putOneRowMultiColumn() {
        // String rowkey = .....   自行设计rowkey
        EasyClient easyClient = init();
        // 构造数据
        final HashMap<String, String> map = new HashMap<>();
        map.put("name", "wang");
        map.put("sex", "boy");
        final RowData rowData = RowData.builder().rowkey("rowkey_2").keyValues(map).build();
        try {
            easyClient.putOneRowMultiColumn(rowData);
            easyClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 4.插入多行多列值测试
     */
    @Test
    public void test_putMultiRowsMultiColumns() {
        // String rowkey = .....   自行设计rowkey
        EasyClient easyClient = init();
        // 构造数据
        final ArrayList<RowData> arrayList = new ArrayList<>();

        final HashMap<String, String> map = new HashMap<>();
        map.put("name", "wang");
        map.put("sex", "boy");
        final RowData rowData = RowData.builder().rowkey("rowkey_3").keyValues(map).build();
        arrayList.add(rowData);

        final HashMap<String, String> map2 = new HashMap<>();
        map2.put("name", "wang");
        map2.put("age", "18");
        final RowData rowData2 = RowData.builder().rowkey("rowkey_4").keyValues(map2).build();
        arrayList.add(rowData2);

        try {
            easyClient.putMultiRowsMultiColumns(arrayList);
            easyClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}