-- @description query01 for PXF test bool data type
SELECT v_text FROM bool_data_type_read_ext_table WHERE id = 3 and v_bool = false;
