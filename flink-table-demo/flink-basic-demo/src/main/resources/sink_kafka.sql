CREATE TABLE user_behavior_sink (
    id STRING,
    behavior STRING,
    num BIGINT,
    ts BIGINT
) with (
    'connector' = 'kafka',
    'topic' = 'user_behavior_sink',
    'properties.bootstrap.servers' = 'localhost:9092',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
)