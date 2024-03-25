-- @description query01 for PXF test json - writable table
INSERT INTO json_write_ext_table SELECT * FROM json_source_table;

SELECT * FROM json_target_table ORDER BY 1;
