-- @description query01 for PXF test back-pressure with POOL_SIZE = 1
INSERT INTO jdbc_bp_write_pool_size_1 SELECT * FROM gp_source_table LIMIT 1000000;
