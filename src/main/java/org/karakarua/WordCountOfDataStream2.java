package org.karakarua;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordCountOfDataStream2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("src/main/resources/words.txt");
        source
                // RichFunction方式
                .flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
                    String jobVariable = null;
                    @Override
                    public void open(Configuration parameters) throws Exception {   // 算子调用前的初始化工作，每次处理都会调用
                        super.open(parameters);
                        jobVariable = "my job: ";
                    }

                    @Override
                    public void close() throws Exception {  // 算子最后一次调用结束后执行，比如算子槽位关闭引起的调用结束
                        jobVariable = null;
                        super.close();
                    }

                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
                        RuntimeContext context = getRuntimeContext();   // 运行环境，可以获取运行过程中的一些参数
                        int index = context.getIndexOfThisSubtask();    // index仅代表flatMap的处理槽位，非最终输出结果的槽位
                        System.out.println(index);
                        Arrays.stream(s.split(" ")).forEach(str -> collector.collect(Tuple2.of(jobVariable + str, 1)));
                    }
                })
                .keyBy((Tuple2<String, Integer> ele) -> ele.f0)
                .sum(1)
                .print();
        env.execute("Word Count2");
    }
}
