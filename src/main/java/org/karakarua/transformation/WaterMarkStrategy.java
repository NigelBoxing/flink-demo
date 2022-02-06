package org.karakarua.transformation;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Watermark的产生是为了应对事件流的EventTime时间乱序的问题（比如由网络延迟引起的EventTime乱序）。
 * WaterMark定义了一种策略，在某个窗口结束时间（取EventTime）往后延迟 n 秒 后 (即延迟到EndTime + n秒) 执行该窗口的聚合计算，
 * 即当出现 EventTime >= 窗口结束时间 + n 的 数据流时，会触发窗口的关闭和计算。
 * <p>
 * Flink实现该策略的方式是： 每流入一条数据就产生一个watermark值（watermark值等于流的EventTime - 容忍的延迟，即 流的eventTime - n），
 * 如果产生的watermark值 >= 窗口的结束时间，则间接表示 当前数据流的EventTime已经达到窗口结束时间 +　n　，　然后关闭窗口并执行计算。
 * </p>
 * <p>
 * watermark值可以周期性产生，也可以按条件产生，在1.11版本之前分别对应与 {@code AssignerWithPunctuatedWatermarks} 和
 * {@code AssignerWithPeriodicWatermarks}, 1.11之后弃用上述方式，由 {@code WatermarkGenerator} 统一实例化。
 * 目前，周期性产生watermark的实现类为 {@code BoundedOutOfOrdernessWatermarks}，
 * 使用方式是 结合{@link WatermarkStrategy#forBoundedOutOfOrderness(Duration)} 和
 * {@link WatermarkStrategy#withTimestampAssigner(TimestampAssignerSupplier)}。
 * 自定义产生watermark的方式是 继承 {@code WatermarkGenerator} ， 然后通过
 * {@code .assignTimestampsAndWatermarks(((WatermarkStrategy<String>) context -> new xxxGenerator()).withTimestampAssigner())} 调用。
 * </p>
 */

public class WaterMarkStrategy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(5000);
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//        SingleOutputStreamOperator<String> watermarks = source.assignTimestampsAndWatermarks(
//                WatermarkStrategy
//                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
//                        .withTimestampAssigner((String element, long recordTimestamp) -> {
//                                    String date_time = element.split(",")[1];
//                                    Date dateTime = null;
//                                    try {
//                                        dateTime = format.parse(date_time);
//                                    } catch (ParseException e) {
//                                        e.printStackTrace();
//                                    }
//                                    return dateTime.getTime();
//                                }
//                        ));
        SingleOutputStreamOperator<String> watermarks = source.assignTimestampsAndWatermarks(
                new AssignerWithPeriodicWatermarks<String>() {
                    long timestamp = 0L;
                    long currentTimeStamp = 0L;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        Watermark watermark = new Watermark(currentTimeStamp - 2000);
                        System.out.println("watermark的时间戳： " + format.format(watermark.getTimestamp()));
                        return watermark;
                    }

                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String date_time = element.split(",")[1];
                        Date dateTime = null;
                        try {
                            dateTime = format.parse(date_time);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        timestamp = dateTime.getTime();
                        currentTimeStamp = Math.max(currentTimeStamp, timestamp);
                        return timestamp;
                    }
                });
        watermarks
                .map((MapFunction<String, Tuple2<String, Integer>>) value -> Tuple2.of(value.split(",")[0], 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .apply((WindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>) (key, window, input, out) -> {
                    // sum 写在重载的apply方法里面是针对相同key的当前TumblingWindow（只有一个Window）的聚合
                    AtomicInteger sum = new AtomicInteger();
                    input.forEach(value -> {
                        sum.addAndGet(value.f1);
                    });
                    SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    // 时间窗口可以取窗口的开始时间和结束时间
                    long start = window.getStart();
                    long end = window.getEnd();
                    String st = format1.format(start);
                    String et = format1.format(end);
                    String outStr =
                            "当前窗口：" + Tuple2.of(key, sum) + ", windowStart: " + st + ", " +
                                    "windowEnd: " + et;
                    out.collect(outStr);
                }).returns(Types.STRING)
                .print();
        // 测试数据：
//        hello,2022-02-06 18:00:01.000
//        hello,2022-02-06 18:00:02.000
//        flink,2022-02-06 18:00:06.000
//        hello,2022-02-06 18:00:03.000
//        hello,2022-02-06 18:00:04.888
//        hello,2022-02-06 18:00:07.000
//        hello,2022-02-06 18:00:12.000
        env.execute("WaterMark Test");
    }
}
