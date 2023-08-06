package org.karakarua.transformation;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * 演示 max和maxBy的区别
 */
public class SimpleTransformation {
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
                Tuple3.of("source", "world", 5),
                Tuple3.of("source", "hello", 6),
                Tuple3.of("source", "apache", 7),
                Tuple3.of("source", "flink", 8)
        );
        DataStreamSource<Tuple3<String, String, Integer>> source = env.fromCollection(collection);
        source
                .keyBy(t -> t.f0)
                // maxBy与max的区别在于maxBy会返回有最大值的元素的所有字段，而max只会返回最大值的字段
                // 尤其是分片依据和最大值依据加起来并不能覆盖元素的全部字段时，max可能不能返回理想结果
                // 所以还是尽量使用maxBy （minBy同理）
//                .maxBy("f2")
                .max("f2")
                .print();
        env.execute("Simple Transformation");
    }
}
