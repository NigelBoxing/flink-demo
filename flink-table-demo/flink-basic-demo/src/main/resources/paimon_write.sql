INSERT INTO user_behavior_paimon
SELECT id, behavior, CAST(1 AS BIGINT) as num, ts
FROM user_behavior