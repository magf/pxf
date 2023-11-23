-- @description query01 for PXF test date with year with more than 4 digits - readable table
SELECT * FROM long_year_read_ext_table ORDER BY 1;

SELECT * FROM long_year_read_legacy_ext_table ORDER BY 1;
