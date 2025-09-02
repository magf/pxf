-- @description query01 for Checking parquet timestamp types: MILLIS, MICROS and NANOS.
SELECT * FROM readable_parquet_table;

SELECT ts_millis FROM readable_parquet_table WHERE ts_millis = timestamp '2025-06-01 00:00:00.123';

SELECT ts_micros FROM readable_parquet_table WHERE ts_micros = timestamp '2025-06-01 00:00:00.123456';

SELECT ts_nanos FROM readable_parquet_table WHERE ts_nanos = timestamp '2025-06-01 00:00:00.123457';