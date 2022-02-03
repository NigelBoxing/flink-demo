package org.karakarua.transformation;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class SimpleTransformation2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设定算子缓存时间，flink通过设定缓存来提高吞吐量，并非真正的来一条处理一条
        // 正整数a：每隔a ms向后面算子发送一批数据，flink默认100 ms，即a=100
        // 0：最低延迟发送，来一条数据就立即向后面算子发送（此方式最消耗网络I/O）
        // -1：不设定缓存时间，只有当缓存满了以后才向后面算子发送，最大延迟发送
        env.setBufferTimeout(100);
        List<Tuple3<String, String, Integer>> collection = Arrays.asList(
                Tuple3.of("source", "hello", 3),
                Tuple3.of("source", "flink", 4),
                Tuple3.of("source", "world", 5)
        );
        List<Tuple3<String, String, Integer>> collection1 = Arrays.asList(
                Tuple3.of("source1", "hello", 5),
                Tuple3.of("source1", "apache", 4),
                Tuple3.of("source1", "flink", 3)
        );
        DataStreamSource<Tuple3<String, String, Integer>> source = env.fromCollection(collection);
        DataStreamSource<Tuple3<String, String, Integer>> source1 = env.fromCollection(collection1);
        // union用于合并两个数据流，但要求两个数据流的内容格式必须完全一致
        DataStream<Tuple3<String, String, Integer>> union = source.union(source1);
        union.print();
        env.execute("Simple Transformation");
    }
}
