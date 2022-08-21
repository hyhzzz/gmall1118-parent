package com.atguigu.gmall.test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author coderhyh
 * @create 2022-08-17 22:39
 * 演示flinkapi intervaljoin
 */
public class Test01_IntervalJoin {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("localhost", 9999)
                .map(lineStr -> {
                    String[] fieldArr = lineStr.split(",");
                    //用对象封装输入内容
                    return new Emp(Integer.parseInt(fieldArr[0]), fieldArr[1], Integer.parseInt(fieldArr[2]), Long.parseLong(fieldArr[3]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Emp>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Emp>() {
                            @Override
                            public long extractTimestamp(Emp element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));

        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("localhost", 8888)
                .map(lineStr -> {
                    String[] fieldArr = lineStr.split(",");
                    return new Dept(Integer.parseInt(fieldArr[0]), fieldArr[1], Long.parseLong(fieldArr[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Dept>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Dept>() {
                            @Override
                            public long extractTimestamp(Dept element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));


        SingleOutputStreamOperator<Tuple2<Emp, Dept>> joinedDS = empDS.keyBy(Emp::getDeptno)
                .intervalJoin(deptDS.keyBy(Dept::getDeptno))
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(new ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>() {
                    @Override
                    public void processElement(Emp left,
                                               Dept right,
                                               ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>.Context ctx,
                                               Collector<Tuple2<Emp, Dept>> out) throws Exception {

                        out.collect(Tuple2.of(left, right));
                    }
                });


        joinedDS.print(">>");


        env.execute();
    }
}
