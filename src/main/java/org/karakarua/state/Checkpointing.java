package org.karakarua.state;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Checkpointing {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000); // 开启checkpoint,并且设定checkpoint间隔为1分钟
        env.getCheckpointConfig().setCheckpointTimeout(600000); // 设定checkpoint运行时间上线为10分钟
    }
}
