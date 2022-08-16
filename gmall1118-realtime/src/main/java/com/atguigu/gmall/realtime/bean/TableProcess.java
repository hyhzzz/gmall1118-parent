package com.atguigu.gmall.realtime.bean;

import lombok.Data;

/**
 * @author coderhyh
 * @create 2022-08-16 7:06
 * 配置表对应的实体类
 */
@Data
public class TableProcess {
    //来源表
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}
