-- @description query01 for PXF test to check that the session is closed correctly
-- start_matchsubs
--
-- m/ERROR:  PXF server error : Failed to read text of query wrong_file_name.*/
-- s/ERROR:  PXF server error : Failed to read text of query wrong_file_name.*/ERROR:  PXF server error : Failed to read text of query wrong_file_name/
--
-- m/HINT:  Check the PXF logs located in the .*/
-- s/HINT:  Check the PXF logs located in the .*/HINT:  Check the PXF logs located in the pxf log dir./
--
-- end_matchsubs
SELECT * FROM named_query_read_ext_table ORDER BY id;

SELECT * FROM named_query_wrong_read_ext_table ORDER BY id;

SELECT * FROM named_query_read_ext_table ORDER BY id;
