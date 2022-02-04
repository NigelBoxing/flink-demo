package org.karakarua.sink;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
        // ------------写入文件（方法已弃用）----------
        source
                .keyBy(t -> t.f0 + "," + t.f1)
                .sum(2)
                // sink的并行度决定了输出的形式：并行度=1，输出单一文件count.txt；并行度>1 目录count.txt下则有N个文件（N取决于实际并行度大小，默认cpu core的数量）
                // 注意：当并行度>1时，目录count.txt下的N个文件中可能有空文件，原因是key的数量小于并行度，并不能全部分散到全部文件
                // 目前writeAsText已被弃用
                .writeAsText("src/main/resources/count.txt", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
        // writeAsCsv只适用于输出Tuple，目前writeAsCsv已被弃用
        // source.writeAsCsv("src/main/source/source.txt");
        // ------------写入端口-------------
        source
                .keyBy(t -> t.f0 + "," + t.f1)
                .sum(2)
                // 写入端口
                .writeToSocket("localhost", 9999, (SerializationSchema<Tuple3<String, String, Integer>>) element -> (element + "\n").getBytes(StandardCharsets.UTF_8));
        // ------------ 自定义输出格式（方法已弃用）----------
        source
                .keyBy(t -> t.f0 + "," + t.f1)
                .sum(2)
                // writeUsingOutputFormat通过configuration、open、writeRecord和close方法自定义输出格式
                // 目前writeUsingOutputFormat已被弃用
                .writeUsingOutputFormat(new OutputFormat<Tuple3<String, String, Integer>>() {
                    private String prefix = null;

                    @Override
                    public void configure(Configuration parameters) {
                        // 赋值输出方式使用的基本字段，如输出到文件时的输出路径（参考FileOutputFormat）
                        // 没有配置的话无需额外编写
                    }

                    @Override
                    public void open(int taskNumber, int numTasks) {
                        // javadoc含义是打开一个并行示例，直接翻译就是每个并行任务处理记录前的操作
                        // numTasks表示最大的并行度，taskNumber表示并行示例索引，taskNumber in [0, numTasks)
                        System.out.println("OutputFormat  taskNumber:" + taskNumber + ", numTasks:" + numTasks);
                        prefix = taskNumber + "> ";
                    }

                    @Override
                    public void writeRecord(Tuple3<String, String, Integer> record) {
                        System.out.println(prefix + record);
                    }

                    @Override
                    public void close() {
                        prefix = null;
                    }
                });

        source
                .keyBy(t -> t.f0 + "," + t.f1)
                .sum(2)
                .addSink(new SinkFunction<Tuple3<String, String, Integer>>() {
                    @Override
                    public void invoke(Tuple3<String, String, Integer> value, Context context) {
                        System.out.println("SinkFunction:" + value);
                    }
                });
        env.execute("Simple Transformation");
    }
}
