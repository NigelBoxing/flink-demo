package org.karakarua.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;

public class WindowTransformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = source
                .map((MapFunction<String, Tuple2<String, Integer>>) value -> {
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    long time = System.currentTimeMillis();
                    String date = format.format(time);
                    int random = (int) (Math.random() * 10);
                    System.out.println("当前时间：" + date + ", 处理值：" + value + ", 随机值：" + random);
                    return Tuple2.of(value, random);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0);

        // ----------------- Tumbling、sliding和session窗口都是基于key的，即window处理的数据都是相同key的数据 -------------------

        // 滚动窗口，基于key的处理记录的时间，每隔10秒输出一次 本10秒内流入的相同key的记录的聚合值
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> timeWindow = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        //         timeWindow.sum(1).print();

        // 滚动窗口，基于key的处理记录的数量，每达到5个相同key的记录时才输出一次 流入的5个相同key的记录的聚合值
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> countWindow = keyedStream.countWindow(5);
        // countWindow.sum(1).print();

        // 滑动窗口，基于key的处理记录的时间，每隔5秒输出一次 过去10秒流入的相同key的记录的聚合值
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> slideTimeWindow =
                keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));
        //        slideTimeWindow.sum(1).print();

        // 滑动窗口，基于key的处理记录的数量，每达到3个相同key的记录使才输出一次 流入的过去的5个相同key的记录的聚合值
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> slideCountWindow = keyedStream.countWindow(5, 3);
        //        slideCountWindow.sum(1).print();

        // 会话窗口，基于key的处理时间的记录，当未来5秒内不再流入相同key的记录， 计算流入的相同key的记录的聚合值
        // 注意：gap不是window的size，而是window之间的size，window的size可以比gap大
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> sessionWindow = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));
        //        sessionWindow.sum(1).print();


        SingleOutputStreamOperator<String> windowApply = keyedStream
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    // 写在重载的apply方法外面（apply算子范围内）是针对相同key的所有TumblingWindow的聚合
                    // AtomicInteger sum = new AtomicInteger();

                    // 注意WindowFunction和apply的参数 泛型的顺序不同
                    // WindowFunction<IN, OUT, KEY, WINDOW>
                    // apply(KEY, WINDOW, Iterable<IN>, Collector<OUT>)
                    // 重载的apply方法中，KEY就是来自于IN，因此KEY == IN.key
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<String> out) {
                        // sum 写在重载的apply方法里面是针对相同key的当前TumblingWindow（只有一个Window）的聚合
                        AtomicInteger sum = new AtomicInteger();
                        input.forEach(value -> {
                            sum.addAndGet(value.f1);
                            // key值来自于value，因此key == value.f0 == value.f0
                            System.out.printf("key: %s, value.getKey: %s, equals: %b%n", key, value.f0,
                                    key.equals(value.f0));
                        });
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        // 时间窗口可以取窗口的开始时间和结束时间
                        long start = window.getStart();
                        long end = window.getEnd();
                        String st = format.format(start);
                        String et = format.format(end);
                        String outStr =
                                "当前窗口：" + Tuple2.of(key, sum) + ", windowStart: " + st + ", " +
                                        "windowEnd: " + et;
                        out.collect(outStr);
                    }
                });
//        windowApply.print();

        SingleOutputStreamOperator<String> countWindowApply = keyedStream
                .countWindow(3)
                .apply((WindowFunction<Tuple2<String, Integer>, String, String, GlobalWindow>) (key, window, input, out) -> {
                    // sum 写在重载的apply方法里面是针对相同key的当前TumblingWindow（只有一个Window）的聚合
                    AtomicInteger sum = new AtomicInteger();
                    input.forEach(value -> {
                        sum.addAndGet(value.f1);
                        // key值来自于value，因此key == value.f0 == value.f0
                        System.out.printf("key: %s, value.getKey: %s, equals: %b%n", key, value.f0,
                                key.equals(value.f0));
                    });
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    // 时间窗口可以取窗口的开始时间和结束时间
                    long maxTimestamp = window.maxTimestamp();
                    String maxTime = format.format(maxTimestamp);
                    String outStr =
                            "当前窗口：" + Tuple2.of(key, sum) + ", maxTimestamp: " + maxTime;
                    out.collect(outStr);
                });
        countWindowApply.print();

        env.execute("Window Test");
    }
}
