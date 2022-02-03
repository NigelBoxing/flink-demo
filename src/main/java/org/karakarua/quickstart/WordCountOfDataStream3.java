package org.karakarua.quickstart;


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
                // processFunction方式，继承于abstractRichFunction
                .process(new ProcessFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) {
                        // The context is only valid during the invocation of this method, do not store it.
                        // ctx.timerService().registerEventTimeTimer(3000);    // context可以理解为处理当前元素时的临时环境
                        Arrays.stream(value.split(" ")).forEach(str -> out.collect(Tuple2.of(str, 1)));
                    }
                })
                .keyBy(t -> t.f0)
                // 使用Reduce的方式计算sum
                .reduce((value1, value2) -> Tuple2.of(value1.f0,value1.f1+value2.f1))
                .print();

        env.execute("Word Count3");
    }
}
