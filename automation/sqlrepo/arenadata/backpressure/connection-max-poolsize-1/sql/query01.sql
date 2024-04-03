-- @description query01 for PXF test back-pressure with jdbc.pool.property.maximumPoolSize = 1
INSERT INTO jdbc_bp_write_connection_max_pool_size_1 SELECT * FROM gp_source_table;
