-- @description query01 for PXF test to check that the session is closed correctly
SELECT * FROM named_query_read_ext_table ORDER BY id;

SELECT * FROM named_query_wrong_read_ext_table ORDER BY id;

SELECT * FROM named_query_read_ext_table ORDER BY id;
