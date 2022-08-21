package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.atguigu.gmall.realtime.utils.MySqlUtil;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author coderhyh
 * @create 2022-08-19 10:36
 * 交易域订单预处理事务事实表
 */
public class DwdTradeOrderPreProcess {
    public static void main(String[] args) {
        // 基本环境准备
        // 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(4);
        // 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设置关联的状态的失效时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        // 检查点相关设置(略)
        // 从kafka的topic_db主题中读取数据创建动态表
        tableEnv.executeSql("create table topic_db(\n" +
                "    `database` string,\n" +
                "    `table` string,\n" +
                "    `type` string,\n" +
                "    `ts` string,\n" +
                "    `data` map<string,string>,\n" +
                "    `old` map<string,string>,\n" +
                "    `proc_time` as proctime()\n" +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_trade_order_pre_process_group"));

        //        tableEnv.executeSql("select * from topic_db").print();

        // 从topic_db动态表中读取订单明细数据
        Table orderDetail = tableEnv.sqlQuery("select\n" +
                "    data['id'] id,\n" +
                "    data['order_id'] order_id,\n" +
                "    data['sku_id'] sku_id,\n" +
                "    data['sku_name'] sku_name,\n" +
                "    data['sku_num'] sku_num,\n" +
                "    data['create_time'] create_time,\n" +
                "    data['source_type'] source_type,\n" +
                "    data['source_id'] source_id,\n" +
                "    data['split_total_amount'] split_total_amount,\n" +
                "    data['split_activity_amount'] split_activity_amount,\n" +
                "    data['split_coupon_amount'] split_coupon_amount,\n" +
                "    cast(cast(data['sku_num'] as decimal(16,2)) * cast(data['order_price']  as decimal(16,2)) as string) split_original_amount,\n" +
                "    `ts` od_ts,\n" +
                "   `proc_time`\n" +
                "from \n" +
                " topic_db\n" +
                "where \n" +
                " `table` = 'order_detail' and `type`='insert'");
        tableEnv.createTemporaryView("order_detail", orderDetail);
        //        tableEnv.executeSql("select * from order_detail").print();

        // 从topic_db动态表中读取订单数据
        Table orderInfo = tableEnv.sqlQuery("select\n" +
                " data['id'] id,\n" +
                "    data['user_id'] user_id,\n" +
                "    data['province_id'] province_id,\n" +
                "    data['operate_time'] operate_time,\n" +
                "    data['order_status'] order_status,\n" +
                "    `type`,\n" +
                "    `old`,\n" +
                "    `ts` oi_ts\n" +
                "from \n" +
                " topic_db\n" +
                "where \n" +
                " `table` = 'order_info' and (`type`='insert' or `type`='update')");
        tableEnv.createTemporaryView("order_info", orderInfo);
        //        tableEnv.executeSql("select * from order_info").print();

        // 从topic_db动态表中读取订单明细活动数据
        Table orderDetailActivity = tableEnv.sqlQuery("select \n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['activity_id'] activity_id,\n" +
                "data['activity_rule_id'] activity_rule_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_activity'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // 从topic_db动态表中读取订单明细优惠券数据
        Table orderDetailCoupon = tableEnv.sqlQuery("select\n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['coupon_id'] coupon_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_coupon'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        // 从MySQL数据库中读取字段表创建动态表
        tableEnv.executeSql(MySqlUtil.getBaseDicDDL());

        // 关联上面5张表，得到订单明细预处理宽表
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "od.id,\n" +
                "od.order_id,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "date_format(od.create_time, 'yyyy-MM-dd') date_id,\n" +
                "od.create_time,\n" +
                "od.source_id,\n" +
                "od.source_type,\n" +
                "od.sku_num,\n" +
                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount,\n" +
                "od.od_ts,\n" +

                "oi.user_id,\n" +
                "oi.province_id,\n" +
                "date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id,\n" +
                "oi.operate_time,\n" +
                "oi.order_status,\n" +
                "oi.`type`,\n" +
                "oi.`old`,\n" +
                "oi.oi_ts,\n" +

                "act.activity_id,\n" +
                "act.activity_rule_id,\n" +

                "cou.coupon_id,\n" +

                "dic.dic_name source_type_name,\n" +

                "current_row_timestamp() row_op_ts\n" +
                "from order_detail od \n" +
                " join order_info oi\n" +
                " on od.order_id = oi.id\n" +
                "left join order_detail_activity act\n" +
                " on od.id = act.order_detail_id\n" +
                "left join order_detail_coupon cou\n" +
                "on od.id = cou.order_detail_id\n" +
                "join base_dic for system_time as of od.proc_time as dic\n" +
                "on od.source_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        //        tableEnv.executeSql("select * from result_table").print();

        // 创建动态表  映射要输出的kakfa主题  dwd_trade_order_pre_process
        tableEnv.executeSql("" +
                "create table dwd_trade_order_pre_process(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "source_id string,\n" +
                "source_type string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "od_ts string,\n" +

                "user_id string,\n" +
                "province_id string,\n" +
                "operate_date_id string,\n" +
                "operate_time string,\n" +
                "order_status string,\n" +
                "`type` string,\n" +
                "`old` map<string,string>,\n" +
                "oi_ts string,\n" +

                "activity_id string,\n" +
                "activity_rule_id string,\n" +

                "coupon_id string,\n" +

                "source_type_name string,\n" +

                "row_op_ts timestamp_ltz(3),\n" +
                "primary key(id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_pre_process"));

        // 将关联结果写到对应的主题中
        tableEnv.executeSql("insert into dwd_trade_order_pre_process select * from result_table ");

    }
}
