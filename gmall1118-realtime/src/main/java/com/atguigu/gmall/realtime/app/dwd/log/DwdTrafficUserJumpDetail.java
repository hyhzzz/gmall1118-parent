package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author coderhyh
 * @create 2022-08-17 11:14
 * 流量域用户跳出明细事务事实表
 */
public class DwdTrafficUserJumpDetail {
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
        String groupId = "dwd_traffic_user_jump_group";
        //创建消费者对象

        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);

        //消费数据 封装流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);

        //对读取的数据进行类型转换 jsonStr ---> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //        jsonObjDS.print(">>>");

        //设置水位线，提取事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        //按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS
                = jsonObjWithWatermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //        keyedDS.print(">>");

        //使用FlinkCEP过滤出跳出数据
        //定义pattern
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            return true;
                        }
                        return false;
                    }
                }
        ).next("second").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            return true;
                        }
                        return false;
                    }
                }
        ).within(Time.seconds(10));

        //将pattern应用到流上
        PatternStream<JSONObject> patternDS = CEP.pattern(keyedDS, pattern);

        //从流中提取数据
        OutputTag<JSONObject> timeoutTag = new OutputTag<JSONObject>("timeoutTag") {
        };

        SingleOutputStreamOperator<JSONObject> filterDS = patternDS.flatSelect(
                timeoutTag,
                new PatternFlatTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<JSONObject> collector) throws Exception {
                        //处理超时数据  注意：虽然使用的是collector.collect向下游传递数据，但是我们是把数据放到了参数1中指定的侧输出流了
                        for (JSONObject jsonObj : map.get("first")) {
                            collector.collect(jsonObj);
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<JSONObject> collector) throws Exception {
                        //处理完全匹配的数据
                        List<JSONObject> jsonObjectList = map.get("first");
                        for (JSONObject jsonObj : jsonObjectList) {
                            collector.collect(jsonObj);
                        }
                    }
                }
        );

        //将完全匹配数据和超时数据进行合并
        DataStream<JSONObject> unionDS = filterDS.union(
                filterDS.getSideOutput(timeoutTag)
        );

        //将合并之后的流输出到kafka主题中
        unionDS.print(">>>>");

        unionDS
                .map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_user_jump_detail"));

        env.execute();
    }
}
