package com.xcar.hbase.common.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;

import java.util.Map;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/9/5
 * \* Time: 18:35
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor

/**
 *  元数据表字段
 */
public class TableMeta {

    // 初始化字段
    private String rowkey; // 表名 nameSpace:tablename 拼接
    private String columnFamily; // 列族
    private int hashNums; // 预分区数
    private String createDate; // 创建日期 yyyy-MM-dd HH:mm:ss
    // 表字段信息
    private String idname; // 表 id 字段
    private String columns; // 表包含列
    private String description; // 表描述信息
}