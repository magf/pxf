-- @description query02 for PXF test date with year with more than 4 digits - writable table
INSERT INTO long_year_write_ext_table SELECT id, birth_date, birth_date_dad FROM long_year_source_table;

SELECT * FROM long_year_target_table ORDER BY 1;

INSERT INTO long_year_write_legacy_ext_table SELECT id, birth_date, birth_date_dad FROM long_year_source_table;

SELECT * FROM long_year_target_legacy_table ORDER BY 1;
