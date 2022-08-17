package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
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

        //        cleanedDS.print("主流数据");

        //将脏数据写到kafka脏数据主题中
        DataStream<String> dirtyDS = cleanedDS.getSideOutput(dirtyTag);

        //        dirtyDS.print("脏数据");

        dirtyDS.addSink(MyKafkaUtil.getKafkaProducer("dirty_data"));


        //新来访客标记修复
        //按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS
                = cleanedDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //修复新访客标记
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(new RichMapFunction<JSONObject, JSONObject>() {

            //声明状态
            //注意：不能在声明的时候直接对状态进行初始化，因为这个时候还获取不到getRuntimeContext
            private ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {

                lastVisitDateState
                        = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastVisitDateState", String.class));

            }

            @Override
            public JSONObject map(JSONObject jsonObj) throws Exception {

                String isNew = jsonObj.getJSONObject("common").getString("is_new");

                //曾经访问日期
                String lastVisitDate = lastVisitDateState.value();
                Long ts = jsonObj.getLong("ts");
                //当前的访问日期  把毫秒数(Long)转换成字符串
                String curVisitDate = DateFormatUtil.toYmdHms(ts);

                if ("1".equals(isNew)) {
                    //新访客
                    if (lastVisitDate == null || lastVisitDate.length() == 0) {
                        //说明是该设备第一次访问，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改
                        lastVisitDateState.update(curVisitDate);

                    } else {
                        if (!lastVisitDate.equals(curVisitDate)) {
                            //且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0
                            isNew = "0";
                            jsonObj.getJSONObject("common").put("is_new", isNew);
                        }
                    }

                } else {
                    //前端标记为老访客
                    if (lastVisitDate == null || lastVisitDate.length() == 0) {
                        //说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序,需要给状态补充一个曾经访问日期，我们这里设置为昨天
                        String yesterday = DateFormatUtil.toYmdHms(ts - 3600 * 1000 * 24);
                        lastVisitDateState.update(yesterday);
                    }

                }
                return jsonObj;
            }
        });
        //        fixedDS.print(">>");


        //分流  错误：错误侧输出流 启动：启动侧输出流  曝光：曝光侧输出流 动作：动作侧输出流 页面：主流
        //定义侧输出流标签
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
        };
        OutputTag<String> errTag = new OutputTag<String>("errTag") {
        };

        //分流逻辑实现
        SingleOutputStreamOperator<String> pageLogDS = fixedDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObj,
                                       ProcessFunction<JSONObject, String>.Context ctx,
                                       Collector<String> out) throws Exception {

                //判断是否是错误日志
                JSONObject errJsonObj = jsonObj.getJSONObject("err");

                if (errJsonObj != null) {
                    //有错误字段，将错误日志发送到错误侧输出流
                    ctx.output(errTag, jsonObj.toJSONString());
                    //从日志中将err属性删除，不需要在往下游传递了
                    jsonObj.remove("err");
                }
                //判断是否是启动日志
                JSONObject startJsonObj = jsonObj.getJSONObject("start");
                if (startJsonObj != null) {
                    //说明是启动日志，将启动日志发送到启动测出流
                    ctx.output(startTag, jsonObj.toJSONString());
                } else {
                    //如果不是启动日志，说明都是属于页面日志，在页面日志还包含了动作、曝光
                    //页面、动作、曝光、都应该包含common、page、ts信息
                    JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                    JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                    Long ts = jsonObj.getLong("ts");

                    //判断是否有曝光行为
                    JSONArray displayArr = jsonObj.getJSONArray("displays");

                    if (displayArr != null) {
                        //说明在页面上有曝光行为
                        //遍历页面中所有的曝光行为
                        for (int i = 0; i < displayArr.size(); i++) {
                            //获取页面一条曝光对象
                            JSONObject displayJsonObj = displayArr.getJSONObject(i);

                            //创建新的json对象 用于存放曝光数据
                            JSONObject resDisplayJsonObj = new JSONObject();
                            resDisplayJsonObj.put("common", commonJsonObj);
                            resDisplayJsonObj.put("page", pageJsonObj);
                            resDisplayJsonObj.put("ts", ts);
                            resDisplayJsonObj.put("display", displayJsonObj);
                            //将曝光数据放到曝光侧输出流中
                            ctx.output(displayTag, resDisplayJsonObj.toJSONString());
                        }
                        jsonObj.remove("displays");
                    }
                    //判断当前页面是否有动作
                    JSONArray actionArr = jsonObj.getJSONArray("actions");
                    if (actionArr != null) {
                        //对动作数组进行遍历
                        for (int i = 0; i < actionArr.size(); i++) {
                            //获取页面上一条动作信息
                            JSONObject actionJsonObj = actionArr.getJSONObject(i);
                            //创建一个新的json对象，用于存放动作数据
                            JSONObject resActionJsonObj = new JSONObject();
                            resActionJsonObj.put("common", commonJsonObj);
                            resActionJsonObj.put("page", pageJsonObj);
                            resActionJsonObj.put("action", actionJsonObj);
                            //将动作数据放到动作侧输出流中
                            ctx.output(actionTag, resActionJsonObj.toJSONString());
                        }
                        jsonObj.remove("actions");
                    }
                    //将页面日志放到主流中
                    out.collect(jsonObj.toJSONString());
                }
            }
        });


        DataStream<String> errDS = pageLogDS.getSideOutput(errTag);
        DataStream<String> startDS = pageLogDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageLogDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageLogDS.getSideOutput(actionTag);

        pageLogDS.print("主流数据");
        errDS.print("错误数据");
        startDS.print("启动数据");
        displayDS.print("曝光数据");
        actionDS.print("动作数据");

        //将不同流的数据写到kafka不同的主题中 作为事实表存在
        pageLogDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_page_log"));
        errDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_error_log"));
        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_display_log"));
        actionDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_action_log"));

        env.execute();
    }
}
