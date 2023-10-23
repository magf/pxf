-- @description query01 for PXF test to check pushdown predicate IN for Oracle
SELECT * FROM predicate_in_oracle_ext_table WHERE id IN (3, 4, 5) ORDER BY 1;
