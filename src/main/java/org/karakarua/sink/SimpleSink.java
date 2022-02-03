package org.karakarua.sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class SimpleSink {
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
                .keyBy(t -> t.f0 + "," + t.f1)
                .sum(2)
                .writeAsText("src/main/resources/count.txt", FileSystem.WriteMode.OVERWRITE)
                // sink的并行度决定了输出的形式：并行度=1，输出单一文件count.txt；并行度>1 目录count.txt下则有N个文件（N取决于实际并行度大小，默认cpu core的数量）
                // 注意：当并行度>1时，目录count.txt下的N个文件中可能有空文件，原因是key的数量小于并行度，并不能全部分散到全部文件
                .setParallelism(4);
        env.execute("Simple Transformation");
    }
}
