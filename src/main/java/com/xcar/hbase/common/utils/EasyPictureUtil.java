package com.xcar.hbase.common.utils;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/6/21
 * \* Time: 16:41
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class EasyPictureUtil {

//    public static void main(String[] args) throws IOException {
//        FileInputStream in = new FileInputStream("D:\\test\\a.png");
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        int len;
//        byte[] bt = new byte[16384];// 16K
//        while ((len = in.read(bt)) != -1) {
//            out.write(bt, 0, len);
//        }
//        byte[] value = out.toByteArray();
//        byte[] dest1 = new byte[10240];
//        byte[] dest2 = new byte[10240];
//        byte[] dest3 = new byte[10240];
//        byte[] dest4 = new byte[10240];
//        byte[] dest5 = new byte[value.length-10240*4];
//        System.arraycopy(value, 0, dest1, 0, 10240);
//        System.arraycopy(value, 10240, dest2, 0, 10240);
//        System.arraycopy(value, 10240*2, dest3, 0, 10240);
//        System.arraycopy(value, 10240*3, dest4, 0, 10240);
//        System.arraycopy(value, 10240*4, dest5, 0, value.length-10240*4);
//
//        FileOutputStream fout = new FileOutputStream("D:\\test\\a4.png");
//        fout.write(dest5);
//        fout.close();
//        in.close();
//    }
}