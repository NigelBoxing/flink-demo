package org.karakarua;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordCountOfDataStream3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("src/main/resources/words.txt");
        source
                .process(new ProcessFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) {
                        // The context is only valid during the invocation of this method, do not store it.
                        ctx.timerService().registerEventTimeTimer(3000);    // context可以理解为处理当前元素时的临时环境
                        Arrays.stream(value.split(" ")).forEach(str -> out.collect(Tuple2.of(str, 1)));
                    }
                })
                .keyBy((Tuple2<String, Integer> ele) -> ele.f0)
                .sum(1)
                .print();
        env.execute("Word Count2");
    }
}
