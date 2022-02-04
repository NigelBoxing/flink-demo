package org.karakarua.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 *
 * <a href="https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/datastream/overview/#data-sources" >Data Sources</a>
 */
public class SimpleSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从特定元素中读取数据
        // 注意这两种方式都不支持并行读入，即并行度都只能为1 （non-parallel）
        DataStreamSource<String> sourceCollection = env.fromCollection(Arrays.asList("hello world", "hello flink", "hello idea"));
        DataStreamSource<String> sourceElements = env.fromElements("hello world", "hello flink", "hello idea");
        // 从特定序列中读取元素，可以支持并行读入 （该方式已过时）
        DataStreamSource<Long> sourceSequence = env.generateSequence(1, 5);
        sourceElements.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) ->
                Arrays.stream(value.split(" ")).forEach(word -> Tuple2.of(word, 1)))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();
        env.execute("Source Operator");
    }
}
