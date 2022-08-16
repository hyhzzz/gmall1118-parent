package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author coderhyh
 * @create 2022-08-16 18:13
 * <p>
 * 流量域未加工的事物事实表--日志数据分流，将不同类型的日志经过分流之后放到kafka不同主题中--作为流量域相关事实表
 */
public class DwdTrafficBaseLogSplit {
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
        String topic = "topic_log";
        String groupId = "dwd_traffic_base_log_group";

        //创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);

        //消费数据，封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);

        // 对流中的数据类型进行转换 jsonStr-->jsonObj 并进行ETL，将脏数据放到侧输出流中
        //定义侧输出流标签
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };

        //转换清洗
        SingleOutputStreamOperator<JSONObject> cleanedDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            //没有发生异常就把转换后的数据放到主流中
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            //如果将字符串转换为json对象的时候，如果发生了异常，说明接收到的字符串不是一个标准的json，作为脏数据处理
                            //将脏数据放到侧输出流中
                            ctx.output(dirtyTag, jsonStr);
                        }

                    }
                });

        cleanedDS.print("主流数据");

        //将脏数据写到kafka脏数据主题中
        DataStream<String> dirtyDS = cleanedDS.getSideOutput(dirtyTag);

        dirtyDS.print("脏数据");

        dirtyDS.addSink(MyKafkaUtil.getKafkaProducer("dirty_data"));


        //新来访客标记修复
        //按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS
                = cleanedDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //修复新访客标记


        //分流  错误：错误侧输出流 启动：启动侧输出流  曝光：曝光侧输出流 动作：动作侧输出流 页面：主流


        //将不同流的数据写到kafka不同的主题中 作为事实表存在

        env.execute();
    }
}
