package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author coderhyh
 * @create 2022-08-17 8:24
 * 流量域独立访客事务事实表
 * uv独立访客过滤 :第一次访问算是独立访客，第二次再来就不算了
 * 每一次访问是pv(页面访问)
 */
public class DwdTrafficUniqueVisitorDetail {


    public static void main(String[] args) throws Exception {

        //指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度 和Kafka分区数保持一致
        env.setParallelism(4);

        //检查点相关设置
        //开启检查点
        //        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        //检查点超时时间 1分钟
        //        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //job取消的之后，检查点是否保留
        //        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //两个检查点之间最小时间间隔 2s
        //        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //状态后端
        //        env.setStateBackend(new HashMapStateBackend());
        //基于内存的状态后端 MemoryStatebackend
        //        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        //远程在hdfs：Fsstatebackend
        //        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/1118/ck");

        //重启策略 重启3次 3秒重启一次  固定
        //        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        //失败率 重启3次 每30天有三次重启的机会  3秒一次  推荐
        //        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));

        //从kafka topic_log主题中读取日志数据
        //声明消费者主题和消费者组
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_detail";

        //创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);

        //消费数据 封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);

        //对读取的数据进行类型转换 jsonstr --> jsonobj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //        jsonObjDS.print(">>");

        //按照mid对数据进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //使用状态编程过滤出独立访客
        SingleOutputStreamOperator<JSONObject> uvDS = keyedDS.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor
                        = new ValueStateDescriptor<>("lastVisitDateState", String.class);

                valueStateDescriptor.enableTimeToLive(
                        //设置TTL 1天 状态过期时间
                        StateTtlConfig.newBuilder(Time.days(1))
                                //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                //.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build()
                );
                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                if (lastPageId != null && lastPageId.length() > 0) {
                    //说明是从其他页面跳转过来的，肯定不是独立访客
                    return false;
                }

                String lastVisitDate = lastVisitDateState.value();
                String curVisitDate = DateFormatUtil.toYmdHms(jsonObj.getLong("ts"));

                if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(curVisitDate)) {
                    //说明今天访问过了
                    return false;
                } else {
                    //以前从来没有访问过
                    lastVisitDateState.update(curVisitDate);
                    return true;
                }
            }
        });

        uvDS.print(">>>");


        //将独立访客保存到kafka的主题中
        uvDS
                .map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_unique_visitor_detail"));

        env.execute();
    }
}
