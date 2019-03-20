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
public class EasyClientTestCreateTable {

    /**
     * 0.初始化
     */
    public EasyClient init() {
        EasyClient client = null;
        try {
            client = EasyClient.init();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return client;
    }

    /**
     * 1.切换表测试
     */
    @Test
    public void test_create() throws IOException {
        EasyClient easyClient = init();
        easyClient.createTable("");
        System.out.println(easyClient.getTableName());
        try {
            easyClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}