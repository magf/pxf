-- @description query01 for PXF test without Oracle date type mapping
SELECT * FROM date_type_without_mapping_ext_table WHERE ts = '2022-02-01 12:01:00.777777';
