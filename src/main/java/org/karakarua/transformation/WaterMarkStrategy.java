package org.karakarua.transformation;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Watermark的产生是为了应对一批事件流的EventTime乱序问题（比如由网络延迟引起的流的顺序并不遵循EventTime有序）。
 * WaterMark定义了一种策略，允许某个窗口结束时间（注：窗口结束时间指的是流EventTime结束的时间）往后延迟 n 秒 后 (即延迟到窗口结束时间EndTime + n秒) 再执行该窗口的聚合计算，
 * 即当出现 流的EventTime >= 窗口结束时间 + n 的 数据流时才认为不会再有比EndTime小的流来了，然后触发窗口的关闭和计算。
 * <p>
 * Flink实现该策略的方式是： 每流入一条数据就产生一个watermark值（watermark值本质是时间戳，等于流的EventTime - 容忍的延迟，即 流的eventTime - n），
 * 当watermark值 >= 窗口的结束时间时，就表示 当前数据流的EventTime已经达到窗口结束时间 +　n　，　然后关闭窗口并执行计算。
 * </p>
 * <p>
 * watermark值可以周期性产生，也可以按条件产生，在1.11版本之前分别对应与 {@code AssignerWithPunctuatedWatermarks},
 * {@code AssignerWithPeriodicWatermarks}, {@code BoundedOutOfOrdernessTimestampExtractor}. 1.11 之后弃用上述方式，由 {@code
 * WatermarkGenerator} 统一实例化。
 * <p>
 * 目前，周期性产生watermark的使用方式是 结合{@link WatermarkStrategy#forBoundedOutOfOrderness(Duration)} 和
 * {@link WatermarkStrategy#withTimestampAssigner(TimestampAssignerSupplier)}。
 * 自定义产生watermark的方式是 继承 {@code WatermarkGenerator} ， 然后通过
 * {@code .assignTimestampsAndWatermarks(((WatermarkStrategy<String>) context -> new xxxGenerator()).withTimestampAssigner())} 调用。
 * </p>
 */

public class WaterMarkStrategy {
    // 测试数据：
//        hello,2022-02-06 18:00:01.000
//        hello,2022-02-06 18:00:02.000
//        flink,2022-02-06 18:00:06.000
//        hello,2022-02-06 18:00:03.000
//        hello,2022-02-06 18:00:04.888
//        hello,2022-02-06 18:00:07.000
//        hello,2022-02-06 18:00:12.000

    static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 注意：使用定时Watermark时设定产生间隔，默认200毫秒
        env.getConfig().setAutoWatermarkInterval(5000);
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        // 定时产生watermark，当watermark的时间戳等于窗口结束时间时将触发计算（watermark时间戳等于事件时间减去容忍时间）
        SingleOutputStreamOperator<String> watermarks = source.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((String element, long recordTimestamp) -> getEventTime(element)
                        ));
        // 过时的定时产生watermark的过程，但可以有助于理解watermark情况下窗口的触发时机（watermark时间戳等于窗口结束时间时）
        SingleOutputStreamOperator<String> watermarks1 = source.assignTimestampsAndWatermarks(
                new AssignerWithPeriodicWatermarks<String>() {
                    long timestamp = 0L;
                    long currentTimeStamp = 0L;

                    @Override
                    public Watermark getCurrentWatermark() {
                        Watermark watermark = new Watermark(currentTimeStamp - 2000);
                        System.out.println("watermark的时间戳： " + format.format(watermark.getTimestamp()));
                        return watermark;
                    }

                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        timestamp = getEventTime(element);
                        currentTimeStamp = Math.max(currentTimeStamp, timestamp);
                        return timestamp;
                    }
                });
        // 自定义实现 PunctuatedWatermarks，允许包含hello内容的流延迟2秒到达，即仅遇到WindowEndTime + 2s的包含hello的消息才关闭窗口。
        WatermarkStrategy<String> watermarkStrategy = context -> new PunctuatedWatermarks(2000);
        SingleOutputStreamOperator<String> watermarks2 = source.assignTimestampsAndWatermarks(watermarkStrategy.withTimestampAssigner(((element, recordTimestamp) -> getEventTime(element))));
        watermarks2
                .map((MapFunction<String, Tuple2<String, Integer>>) value -> Tuple2.of(value.split(",")[0], 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .apply((WindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>) (key, window, input, out) -> {
                    // sum 写在重载的apply方法里面是针对相同key的当前TumblingWindow（只有一个Window）的聚合
                    AtomicInteger sum = new AtomicInteger();
                    input.forEach(value -> sum.addAndGet(value.f1));
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
        env.execute("WaterMark Test");
    }

    /**
     * 自实现punctuatedWatermark, 只有流中包含hello时才产生watermark，且 watermark时间戳取（事件事件 - 容忍时间）
     * 可以参考 :
     * <a href = "https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/datastream/event-time/generating_watermarks/">生成watermark</a>
     */
    static class PunctuatedWatermarks implements WatermarkGenerator<String> {

        long delay;

        public PunctuatedWatermarks(long delay) {
            this.delay = delay;
        }

        @Override
        public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
            String[] eventContent = StringUtils.split(event, ",");
            if (eventContent[0].equals("hello")) {
                // 只有数据第一个字段等于hello才产生watermark
                long eventTime = getEventTime(event);
                output.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(eventTime - delay));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 不需要实现周期发送watermark
        }
    }

    public static long getEventTime(String element) {
        String date_time = element.split(",")[1];
        Date dateTime = null;
        try {
            dateTime = format.parse(date_time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if (dateTime != null) {
            return dateTime.getTime();
        }
        return Long.MIN_VALUE;
    }
}
