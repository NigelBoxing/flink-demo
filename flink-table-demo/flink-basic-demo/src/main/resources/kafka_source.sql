CREATE TABLE user_behavior (
    id STRING,
    behavior STRING,
    ts BIGINT
) with (
    'connector' = 'kafka',
    'topic' = 'user_behavior',
    'properties.bootstrap.servers' = 'localhost:9092',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
)