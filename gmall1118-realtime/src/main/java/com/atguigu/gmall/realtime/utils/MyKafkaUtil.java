package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import javax.annotation.Nullable;

/**
 * @author coderhyh
 * @create 2022-08-15 22:55
 * kafka工具类
 */
public class MyKafkaUtil {

    private static final String KAFKA_SERVER = "hadoop102:9092,hadoop102:9092,hadoop103:9092";

    //获取消费者对象的方法
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {


        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        //消费字符串的话 这个是没有问题的，如果读取到null数据的话，就有问题了
        //        new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),props);

        //我们需要自定义反序列化 对空值进行处理
        return new FlinkKafkaConsumer<String>(topic, new KafkaDeserializationSchema<String>() {
            //是流的最后一个元素？
            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            //consumerRecord ：要消费的一条数据
            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                if (consumerRecord != null && consumerRecord.value() != null) {
                    //有值的话就把当前的值封装成字符串
                    return new String(consumerRecord.value());
                }
                //如果是null数据的话，就返回空
                return null;
            }

            //获取类型，当前流中处理的是什么类型
            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        }, props);

    }

    //获取生产者对象的方法

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        //设置生产者事务超时时间
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "");

        return new FlinkKafkaProducer<String>("default",
                //自己实现序列化
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
                        return new ProducerRecord<byte[], byte[]>(topic, s.getBytes());
                    }
                }
                //设置精准一次语义
                , props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
