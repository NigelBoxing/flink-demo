package org.karakarua.client;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableSqlDemoMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String demoSourceSql = "CREATE TABLE demoSource ( " +
                "user_info STRING " +
                ") WITH ( " +
                "'connector' = 'kafka'," +
                " 'topic' = 'demo_user_behavior', " +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                " 'scan.startup.mode' = 'latest-offset', " +
                "'format' = 'json' " +
                ")";
        System.out.println(demoSourceSql);
        String demoSinkSql = "CREATE TABLE demoSink (" +
                " user_id STRING," +
                " user_pv INT," +
                " user_click INT, " +
                "ts BIGINT " +
                ") WITH ( " +
                "'connector' = 'kafka', " +
                "'topic' = 'demo_user_behavior_sink', " +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                " 'scan.startup.mode' = 'earliest-offset', " +
                "'format' = 'json' )";
        System.out.println(demoSinkSql);
        String planSql = "INSERT INTO `demoSink` " +
                "SELECT SPLIT_INDEX(`user_info`, '#', 0) AS `user_id`, " +
                "CAST(SPLIT_INDEX(`user_info`, '#', 1) AS INT) AS `user_pv`, " +
                "CAST(SPLIT_INDEX(`user_info`, '#', 2) AS INT) AS `user_click`, " +
                "CAST(SPLIT_INDEX(`user_info`, '#', 3) AS BIGINT)AS `ts` " +
                "FROM demoSource";
        System.out.println(planSql);
        tableEnv.executeSql(demoSourceSql);
        tableEnv.executeSql(demoSinkSql);
        tableEnv.executeSql(planSql);
    }
}
