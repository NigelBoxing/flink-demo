INSERT INTO user_behavior_sink
SELECT id, behavior, CAST(1 AS BIGINT) as num, ts
FROM user_behavior