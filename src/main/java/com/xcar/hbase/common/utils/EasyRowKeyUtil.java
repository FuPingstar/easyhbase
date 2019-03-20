package com.xcar.hbase.common.utils;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/8/16
 * \* Time: 13:49
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class EasyRowKeyUtil {

    /** 采用 hash 散列分区*/
    public static String hashRowkey(String originalKey, int hashNums){
        if(hashNums==0){
            return originalKey;
        }else {
            return String.format("%02x", Math.abs(originalKey.hashCode()) % (hashNums+1)) + "_" + originalKey;
        }
    }

    /** rowkey 反转 */
    public static String reverseRowkey(String originalKey) {
        StringBuilder sb = new StringBuilder(originalKey.length());
        for (int i = originalKey.length() - 1; i >= 0; i--) {
            sb.append(originalKey.toCharArray()[i]);
        }
        return sb.toString();
    }
}