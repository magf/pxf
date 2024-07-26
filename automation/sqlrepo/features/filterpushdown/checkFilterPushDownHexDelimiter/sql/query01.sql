-- @description query01 for PXF filter pushdown with hex delimiter
SET gp_external_enable_filter_pushdown = true;

SET optimizer = off;

SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 = 'C' AND a1 = 2 ORDER BY t0, a1;

SET optimizer = on;

SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 = 'C' AND a1 = 2 ORDER BY t0, a1;
