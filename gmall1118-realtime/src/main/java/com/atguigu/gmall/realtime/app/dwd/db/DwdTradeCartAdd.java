package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.atguigu.gmall.realtime.utils.MySqlUtil;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author coderhyh
 * @create 2022-08-18 19:21
 * 交易域加购事务事实表
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {

        // 基本环境准备
        // 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(4);

        // 设置表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置关联的时候状态的过期时间
        //        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        // 检查点相关设置
        // 从kafka topic_db主题中读取业务数据 创建动态表
        // topic_db主题中的数据，映射成表，数据中json属性要映射成表中的字段，所以字段要和属性名一样
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` string,\n" +
                "  `table` string,\n" +
                "  `type` string,\n" +
                "  `ts` STRING,\n" +
                "  `proc_time` as proctime(),\n" +
                "  `data` MAP<string, string>,\n" +
                "  `old` MAP<string, string>\n" +
                " )" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_trade_cart_add_group"));

        //        tableEnv.executeSql("select * from topic_db").print();

        // 从上面创建的动态表中过滤出加购数据 ---> 还是动态表
        // 从data中过滤 字段是MySQL cat_info表的字段
        Table cartAdd = tableEnv.sqlQuery("select \n" +
                "    data['id'] id,\n" +
                "    data['user_id'] user_id,\n" +
                "    data['sku_id'] sku_id,\n" +
                "    data['source_type'] source_type,\n" +
                "    data['source_id'] source_id,\n" +
                "    if(`type`='insert',\n" +
                "    data['sku_num'],\n" +
                " cast((cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string)) sku_num,\n" +
                " ts,proc_time\n" +
                "from \n" +
                "    topic_db\n" +
                "where\n" +
                "    `table` = 'cart_info'\n" +
                " and\n" +
                " (`type` = 'insert' " +
                "or (`type` = 'update' and `old`['sku_num'] is not null and cast(data['sku_num'] as int) > cast(`old`['sku_num'] as int)))");

        tableEnv.createTemporaryView("cart_add", cartAdd);
        //        tableEnv.executeSql("select * from cart_add").print();

        // 从数据库中读取字典表数据创建动态表
        tableEnv.executeSql(MySqlUtil.getBaseDicDDL());
        //        tableEnv.executeSql("select * from base_dic").print();

        // 关联两张动态表   (加购表/字典表)
        Table resTable = tableEnv.sqlQuery("select\n" +
                " id,user_id,sku_id,source_id,source_type,dic_name source_type_name,sku_num,ts\n" +
                "from\n" +
                " cart_add cadd\n" +
                " join\n" +
                " base_dic FOR SYSTEM_TIME AS OF cadd.proc_time AS  dic\n" +
                " on\n" +
                " cadd.source_type = dic.dic_code");
        tableEnv.createTemporaryView("res_table", resTable);

        //        tableEnv.executeSql("select * from res_table").print();

        // 创建动态表和要写出的dwd进行关联
        // 字段是上面查询出来的字段
        tableEnv.executeSql("CREATE TABLE dwd_trade_cart_add (\n" +
                "  id string,\n" +
                "  user_id string,\n" +
                "  sku_id string,\n" +
                "  source_id string,\n" +
                "  source_type string,\n" +
                "  source_type_name string,\n" +
                "  sku_num string,\n" +
                "  ts string,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") " + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_cart_add"));

        //将关联的数据写到 kafka输出主题对应的动态表中
        tableEnv.executeSql("insert into dwd_trade_cart_add select * from res_table");

    }
}
