-- @description query02 for PXF test json - readable table
SELECT * FROM postgres_json_read_ext_table ORDER BY 1;

SELECT * FROM postgres_json_read_ext_table WHERE data_json IS NULL;
