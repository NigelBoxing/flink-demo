CREATE TABLE user_behavior_paimon (
    id STRING,
    behavior STRING,
    num BIGINT,
    ts BIGINT,
    PRIMARY KEY (id,behavior) NOT ENFORCED
) with (
    'connector' = 'paimon',
    'path' = 'file:/tmp/paimon/tables/user_behavior_paimon',
    'auto-create' = 'true',
    'merge-engine' = 'aggregation',
    'fields.num.aggregate-function' = 'sum',
    'sequence.field' = 'ts',
    'changelog-producer' = 'lookup'
)