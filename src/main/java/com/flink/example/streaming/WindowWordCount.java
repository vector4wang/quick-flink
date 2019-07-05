package com.flink.example.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author vector
 * @date: 2019/7/5 0005 17:01
 */
public class WindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<Tuple2<String, Integer>> dataStream = env.socketTextStream("192.168.1.33", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {// 竟然不允许省略
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
                        for (String word : s.split(" ")) {
                            collector.collect(new Tuple2<>(word, 1));
                        }
                    }
                }).keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        dataStream.print();
        System.out.println("=========================");
        env.execute();
    }

}
