-- @description query01 for PXF filter pushdown disabled case

SET gp_external_enable_filter_pushdown = off;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE  t0 = 'C' AND a1 = 2 ORDER BY t0, a1;
