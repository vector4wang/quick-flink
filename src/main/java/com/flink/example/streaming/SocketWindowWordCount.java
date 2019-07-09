package com.flink.example.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 比较重要的就是如何打包
 *
 * @author vector
 * @date: 2019/7/9 0009 14:34
 */
public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception {
        final String host;
        final int port;

        try {
            final ParameterTool parameterTool = ParameterTool.fromArgs(args);
            host = parameterTool.has("hostname") ? parameterTool.get("hostname") : "localhost";
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.out.println("No port specified! please run 'SocketWindowWordCount --port <port>'");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream(host, port);

        DataStream<WordWithCount> windowCounts = text
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String s, Collector<WordWithCount> collector) {
                        for (String s1 : s.split("\\s")) {
                            collector.collect(new WordWithCount(s1, 1L));
                        }
                    }
                })
                .keyBy("word")
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount Reset!!!");

    }


    public static class WordWithCount {
        public String word;
        private long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
