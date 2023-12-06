package org.karakarua.client;

import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class KafkaDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String classpath = KafkaDemo.class.getClassLoader().getResource("").getPath();
        String demoSourceSql =
                FileUtils.readFileToString(new File(classpath.concat("kafka_source.sql")), StandardCharsets.UTF_8);
        System.out.println(demoSourceSql);
        String demoSinkSql = FileUtils.readFileToString(new File(classpath.concat("sink_kafka.sql")), StandardCharsets.UTF_8);
        System.out.println(demoSinkSql);
        String planSql = FileUtils.readFileToString(new File(classpath.concat("demoSql.sql")), StandardCharsets.UTF_8);
        System.out.println(planSql);
        tableEnv.executeSql(demoSourceSql);
        tableEnv.executeSql(demoSinkSql);
        tableEnv.executeSql(planSql);
    }
}
