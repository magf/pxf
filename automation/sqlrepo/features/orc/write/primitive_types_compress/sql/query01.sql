-- @description query01 for writing primitive ORC data types with compress
SET bytea_output=hex;

SET timezone='America/Los_Angeles';
SELECT * FROM pxf_orc_view ORDER BY id;

SET timezone='America/New_York';
SELECT * FROM pxf_orc_view ORDER BY id;
