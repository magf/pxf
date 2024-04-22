-- @description query01 for PXF test back-pressure with the parameter BATCH_TIMEOUT = 2
INSERT INTO jdbc_bp_write_batch_timeout_error SELECT * FROM gp_source_table;
