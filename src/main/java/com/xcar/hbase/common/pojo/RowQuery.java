package com.xcar.hbase.common.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/6/15
 * \* Time: 18:29
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RowQuery {
    String rowkey;
    List<String> columns;
}