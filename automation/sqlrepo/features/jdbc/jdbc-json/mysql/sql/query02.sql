-- @description query02 for PXF test json - readable table
SELECT * FROM mysql_json_read_ext_table ORDER BY 1;

SELECT * FROM mysql_json_read_ext_table WHERE data_jsonb IS NULL;
