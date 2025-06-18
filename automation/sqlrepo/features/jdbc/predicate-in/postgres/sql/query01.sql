-- @description query01 for PXF test to check pushdown predicate IN for Postgres
SELECT * FROM predicate_in_pg_ext_table WHERE id IN (2, 3) ORDER BY 1;
