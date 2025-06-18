-- @description query01 for PXF test json - writable table
INSERT INTO postgres_json_write_ext_table SELECT * FROM json_source_table;

SELECT * FROM postgres_json_target_table ORDER BY 1;

SELECT * FROM postgres_json_target_table WHERE data_jsonb IS NULL;
