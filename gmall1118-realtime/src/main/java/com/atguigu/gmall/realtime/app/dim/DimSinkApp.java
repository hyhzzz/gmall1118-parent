package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import com.atguigu.gmall.realtime.app.func.DimSinkFunction;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author coderhyh
 * @create 2022-08-15 17:07
 * 维度处理类
 * 数仓Dim层创建
 */
public class DimSinkApp {
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

        //从kafka topic_db中消费业务数据
        //声明消费者主题和消费者组
        String topic = "topic_db";
        String groupId = "dim_sink_group";

        //创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);

        //消费数据 封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);

        //对读取的数据进行类型转换 JsonStr --> JsonObj
        //输入是 jsonstr  输出格式是jsonobj


        /* kafkaStrDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String jsonStr) throws Exception {
                return JSON.parseObject(jsonStr);
            }
        });*/

        //        kafkaStrDS.map(jsonStr-> JSON.parseObject(jsonStr));
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        //对读取的数据进行ETL--->主流
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        try {
                            String dataJsonStr = jsonObj.getString("data");

                            //判断本身是不是一个标准json字符串
                            JSONValidator.from(dataJsonStr).validate();
                            if (jsonObj.getString("type").equals("bootstrap-start") || jsonObj.getString("type").equals("bootstrap-complete")) {
                                return false;
                            }
                            //ture的话就是一个正常的json
                            return true;
                        } catch (Exception e) {
                            //如果发生异常就把数据过滤掉
                            return false;
                        }
                    }
                }
        );

        //        filterDS.print(">>>");


        //使用FlinkCDC 读取MySQL配置表数据 ---> 配置流

        //cdc连接报错，说是连接有问题 和ssl有关系 尝试这样解决
        //        Properties props = new Properties();
        //        props.setProperty("jdbc.properties.useSSL","false");

        /* //来源表
        String sourceTable;
        //输出表
        String sinkTable;
        //输出字段
        String sinkColumns;
        //主键字段
        String sinkPk;
        //建表扩展
        String sinkExtend;*/
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall1118-realtime")
                .tableList("gmall1118-realtime.table_process")
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        // 使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS =
                env.fromSource(
                        mySqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "MysqlSource");

        //        mysqlDS.print(">>>");

        //将配置流进行广播 -->广播流
        //声明广播状态描述器 存储配置表的数据  k:是表名   v：是tableprocess
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = mysqlDS.broadcast(mapStateDescriptor);

        //将主流和广播流关联到一起
        BroadcastConnectedStream<JSONObject, String> connectedDS = filterDS.connect(broadcastDS);

        //分别对两条流进行处理--->从主流中把维度数据过滤出来  主流(左流)  配置流(右流)  输出的类型数据
        SingleOutputStreamOperator<JSONObject> dimDS = connectedDS.process(new TableProcessFunction(mapStateDescriptor));

        dimDS.print("维度数据");

        //将维度数据写到phoenix表中
        dimDS.addSink(new DimSinkFunction());

        env.execute();
    }
}
