-- @description query01 for PXF test to check that the session is closed correctly

-- start_matchsubs
--
-- m/.*File '\/usr\/local\/.*\/pxf\/servers\/named\/wrong_file_name.sql.*/
-- s/.*File '\/usr\/local\/.*\/pxf\/servers\/named\/wrong_file_name.sql.*/ERROR:  PXF server error : Failed to read text of query wrong_file_name : File '\/usr\/local\/greenplum-db-devel\/pxf\/servers\/named\/wrong_file_name.sql' does not exist/
--
-- end_matchsubs

SELECT * FROM named_query_read_ext_table ORDER BY id;

SELECT * FROM named_query_wrong_read_ext_table ORDER BY id;

SELECT * FROM named_query_read_ext_table ORDER BY id;
