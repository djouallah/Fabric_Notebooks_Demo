-- materialized: (mstdatetime,ignore)
WITH data AS (
    SELECT
        UNNEST(GENERATE_SERIES('2023-01-01 00:00:00'::TIMESTAMPTZ, '2023-01-01 23:55:00'::TIMESTAMPTZ, INTERVAL 5 MINUTE)) AS interval_time
)
SELECT
    CAST(STRFTIME(interval_time, '%H%M') AS INT16) AS time,
    CAST(STRFTIME(interval_time, '%H') AS INT16) AS hour
FROM data;
