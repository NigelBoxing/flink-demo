package org.karakarua.quickstart;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordCountOfDataStream4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("flink-stream-demo/src/main/resources/words.txt");
        // 可以统一设定整个环境（所有的算子）的并行度，也可以对算子单独设定并行度
        // env.setParallelism(3);
        source
                // processFunction方式
                .process(new ProcessFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) {
                        // The context is only valid during the invocation of this method, do not store it.
                        // ctx.timerService().registerEventTimeTimer(3000);    // context可以理解为处理当前元素时的临时环境
                        Arrays.stream(value.split(" ")).forEach(str -> out.collect(Tuple2.of(str, 1)));
                    }
                })
                // 单独设置算子的并行度
                .setParallelism(3)
                .keyBy(t -> t.f0)
                // 使用keyedFunction的方式计算sum
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    private int sum = 0;

                    @Override
                    public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) {
                        String key = ctx.getCurrentKey();
                        sum += value.f1;
                        System.out.println("processing value = " + value + ", sum = " + sum);
                        out.collect(Tuple2.of(key, sum));
                    }
                })
                .setParallelism(2)
                .print();

        env.execute("Word Count4");
    }
}
