package org.karakarua.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;

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

        // 滚动窗口，基于处理记录时的时间，每隔10秒输出一次 本10秒内流入的记录的聚合值
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> timeWindow = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // timeWindow.sum(1).print();

        // 滚动窗口，基于处理记录的数量，每达到5个记录时才输出一次 流入的5个记录的聚合值
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> countWindow = keyedStream.countWindow(5);
        // countWindow.sum(1).print();

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> slideTimeWindow =
                keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));
        // 滑动窗口，基于处理记录的时间，每隔5秒输出一次 过去10秒流入的记录的聚合值
        // slideTimeWindow.sum(1).print();

        // 滑动窗口，基于处理记录的数量，每达到3个记录使才输出一次 流入的过去的5个记录的聚合值
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> slideCountWindow = keyedStream.countWindow(5, 3);
        slideCountWindow.sum(1).print();

        env.execute("Window Test");
    }
}
