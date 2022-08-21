package com.atguigu.gmall.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author coderhyh
 * @create 2022-08-18 16:07
 * 演示Flink JDBC Connector 以及lookup join
 * 从kafka中主题中获取员工信息，和mysql数据库中的部门维度表进行关联
 */
public class Test03_JdbcConnector {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // 从kafka主题中读取数据，创建动态表
        tableEnv.executeSql("CREATE TABLE emp (\n" +
                "  empno BIGINT,\n" +
                "  ename string,\n" +
                "  deptno bigint,\n" +
                "  proc_time as proctime()\n" + //处理时间字段
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'first',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");


        //从MySQL数据库中查询部门信息  创建动态表
        tableEnv.executeSql("CREATE TABLE dept (\n" +
                "  deptno bigint,\n" +
                "  dname string,\n" +
                "  ts bigint\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop102:3306/gmall1118_config',\n" +
                "   'table-name' = 't_dept',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456',\n" +
                "   'lookup.cache.max-rows' = '200',\n" +
                "   'lookup.cache.ttl' = '1 hour',\n" +
                "   'driver'='com.mysql.cj.jdbc.Driver'" +
                ")");

        //SYSTEM_TIME:指定时间标记
        tableEnv.executeSql("SELECT e.empno,e.ename,e.deptno,d.dname FROM emp e JOIN dept FOR SYSTEM_TIME AS OF e.proc_time d ON e.deptno = d.deptno").print();
    }
}
