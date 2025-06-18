-- @description query01 for Checking Timestamp with session timezone.
SET timezone = 'Europe/Moscow';
SELECT * FROM readable_parquet_table;