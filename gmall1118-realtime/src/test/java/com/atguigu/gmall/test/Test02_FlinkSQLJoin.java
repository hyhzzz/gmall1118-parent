package com.atguigu.gmall.test;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author coderhyh
 * @create 2022-08-17 22:39
 * 演示flinksql join
 */
public class Test02_FlinkSQLJoin {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //状态过期时间
        //        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("localhost", 9999)
                .map(lineStr -> {
                    String[] fieldArr = lineStr.split(",");
                    //用对象封装输入内容
                    return new Emp(Integer.parseInt(fieldArr[0]), fieldArr[1], Integer.parseInt(fieldArr[2]), Long.parseLong(fieldArr[3]));
                });

        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("localhost", 8888)
                .map(lineStr -> {
                    String[] fieldArr = lineStr.split(",");
                    return new Dept(Integer.parseInt(fieldArr[0]), fieldArr[1], Long.parseLong(fieldArr[2]));
                });


        //将流转换为动态表
        tableEnv.createTemporaryView("emp", empDS);
        tableEnv.createTemporaryView("dept", deptDS);


        //内连接
        //        tableEnv.executeSql("select e.empno,e.ename,e.deptno,d.dname from  emp e join dept d on e.deptno = d.deptno").print();


        //左外连接
        //如果有两张表进行左外连接，左表数据过来，如果右表数据还没到，会生成一条右表字段为null的数据，标记为+I
        //然后右表的数据到了，会将之前的数据撤回，会生成一条和第一条数据内容相同，但是标记为-D的数据
        //在生成一条关联后的数据，标记为+I
        //        tableEnv.executeSql("select e.empno,e.ename,e.deptno,d.dname from  emp e left join dept d on e.deptno = d.deptno").print();


        //右外连接
        //tableEnv.executeSql("select e.empno,e.ename,e.deptno,d.dname from emp e right join dept d on e.deptno = d.deptno").print();


        //全外连接
        //tableEnv.executeSql("select e.empno,e.ename,e.deptno,d.dname from emp e full join dept d on e.deptno = d.deptno").print();

        //将左外连接的结果，写到kafka主题
        //创建动态表，和kafka主题关联
        //官网table api 第一个kakfa一般用于我们消费kafka数据。upsert kafka都是作为生产者 往kafka里面写数据
        tableEnv.executeSql(" CREATE TABLE kafka_emp (\n" +
                "        empno  BIGINT,\n" +
                "        enmae  STRING,\n" +
                "        deptno BIGINT,\n" +
                "        dname  STRING\n" +
                "        PRIMARY KEY (empno) NOT ENFORCED" +
                "  ) WITH (\n" +
                "        'connector' = 'upsert-kafka',\n" +
                "        'topic' = 'first',\n" +
                "        'properties.bootstrap.servers' = 'hadoop202:9092',\n" +
                "        'key.format' = 'json',\n" +
                "        'value.format' = 'json'\n" +
                ")");

        //向表中添加数据
        tableEnv.executeSql("insert into kafka_emp select e.empno,e.ename,e.deptno,d.dname from  emp e left join dept d on e.deptno = d.deptno ");


        env.execute();
    }
}
