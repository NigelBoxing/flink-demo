package org.karakarua;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class VisualDAG {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 可以从flink UI上看到任务的DAG图
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordMap =
                source.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) ->
                        Arrays.stream(value.split(" ")).forEach(word -> Tuple2.of(word, 1))
                ).returns(Types.TUPLE(Types.STRING, Types.INT));
        wordMap.keyBy(t -> t.f0)
                .sum(1)
                .print();
        String executionPlan = env.getExecutionPlan();
        // 去https://flink.apache.org/visualizer/ 上查看flinkDAG图
        System.out.println(executionPlan);
        env.execute("Visual Plan");
    }
}
