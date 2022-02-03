package org.karakarua.transformation;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class KeyType {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("src/main/resources/words.txt");
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordMap = source
                .process(new ProcessFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) {
                        // The context is only valid during the invocation of this method, do not store it.
                        // ctx.timerService().registerEventTimeTimer(3000);    // context可以理解为处理当前元素时的临时环境
                        Arrays.stream(value.split(" ")).forEach(str -> out.collect(Tuple2.of(str, 1)));
                    }
                })
                .setParallelism(3);
        // keyBy(int... fields) 通过fields的位置定位key
        // keyBy(String... fields) 通过fields的名称定位key
        // 目前以上两种方式均在1.12及后续版本被抛弃，不建议使用
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream1 = wordMap.keyBy("f0");
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream2 = wordMap.keyBy(0);
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream3 = wordMap.keyBy("f0","f1");

        // keyBy(int... fields)和keyBy(String... fields)返回的KeyedStream的KEY（KeyedStream的第二个泛型）是Tuple，形式为Tuple<key_type>
        // 输出 Java Tuple1<String>
        TypeInformation<Tuple> keyType = keyedStream1.getKeyType();
        System.out.println(keyType);
        // 输出 Java Tuple1<String>
        TypeInformation<Tuple> keyType1 = keyedStream2.getKeyType();
        System.out.println(keyType1);
        // 输出 Java Tuple2<String, Integer>
        TypeInformation<Tuple> keyType2 = keyedStream3.getKeyType();
        System.out.println(keyType2);

        // 推荐使用KeySelector的方式
        KeyedStream<Tuple2<String, Integer>, String> keyedWords = wordMap.keyBy(t -> t.f0);
        // 输出String
        System.out.println(keyedWords.getKeyType());
        env.execute("Word Count4");
    }
}
