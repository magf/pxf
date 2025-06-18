-- @description query01 for PXF test back-pressure with the parameter BATCH_TIMEOUT = 120
INSERT INTO jdbc_bp_write_batch_timeout_success SELECT * FROM gp_source_table;
