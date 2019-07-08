package com.flink.example.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author vector
 * @date: 2019/7/8 0008 19:52
 */
public class WordCountExample {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> text = env.fromElements("Who's there?", "I think i hear them. Stand, ho ! who's there");
        DataSet<Tuple2<String, Integer>> wordCounts = text.flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);

        wordCounts.print();


    }

    private static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : s.split(" ")) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
