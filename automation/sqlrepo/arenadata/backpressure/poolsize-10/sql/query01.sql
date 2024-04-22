-- @description query01 for PXF test back-pressure with POOL_SIZE = 10
INSERT INTO jdbc_bp_write_pool_size_10 SELECT * FROM gp_source_table LIMIT 3000000;
