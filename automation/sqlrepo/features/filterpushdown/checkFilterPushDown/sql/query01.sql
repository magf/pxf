-- @description query01 for PXF filter pushdown case using the planner
--
-- start_matchsubs
--
-- # filter values that are equivalent but have different operand order
--
-- m/a0c25s1dCo5a1c23s1d2o3l0/
-- s/a0c25s1dCo5a1c23s1d2o3l0/a1c23s1d2o3a0c25s1dCo5l0/
--
-- m/a0c25s1dCo5a1c23s1d2o5a1c23s2d10o5l1l0/
-- s/a0c25s1dCo5a1c23s1d2o5a1c23s2d10o5l1l0/a1c23s1d2o5a1c23s2d10o5l1a0c25s1dCo5l0/
--
-- m/a2c16s4dtrueo0l2a1c23s1d5o1l0/
-- s/a2c16s4dtrueo0l2a1c23s1d5o1l0/a1c23s1d5o1a2c16s4dtrueo0l2l0/
--
-- m/a1o8a0c25s1dBo5l0/
-- s/a1o8a0c25s1dBo5l0/a0c25s1dBo5a1o8l0/
--
-- m/o8l2/
-- s/o8l2/o9/
--
-- end_matchsubs

-- make sure the pushdown is enabled for this test
SET gp_external_enable_filter_pushdown = true;

-- control - no predicates
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter;

SET optimizer = off;

-- test logical predicates
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 = 'B' AND a1 IS NULL ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 = 'C' AND a1 = 2 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 = 'C' AND a1 <= 2 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 = 'C' AND (a1 = 2 OR a1 = 10) ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 = 'C' OR (a1 >= 0 AND a1 <= 2) ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE b2 = false ORDER BY t0, a1;
SELECT t0, a1, filtervalue FROM test_filter WHERE a1 < 5 AND b2 = false ORDER BY t0, a1;
SELECT round(sqrt(a1)::numeric,5), filtervalue FROM test_filter WHERE a1 < 5 AND b2 = false ORDER BY t0, a1;
SELECT round(sqrt(a1)::numeric,5), filtervalue FROM test_filter WHERE b2 = false ORDER BY t0;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE b2 = false AND (a1 = 3 OR a1 = 10) ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE b2 = false OR (a1 >= 0 AND a1 <= 2) ORDER BY t0, a1;

-- test text predicates
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 =  'C' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 =  'C ' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 <  'C' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 <= 'C' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 >  'C' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 >= 'C' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 <> 'C' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 LIKE     'C%' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 NOT LIKE 'C%' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 IN     ('C','D') ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 NOT IN ('C','D') ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 BETWEEN     'B' AND 'D' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 NOT BETWEEN 'B' AND 'D' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 IS NULL ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE t0 IS NOT NULL ORDER BY t0, a1;

-- test integer predicates
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE a1 =  2 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE a1 <  2 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE a1 <= 2 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE a1 >  2 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE a1 >= 2 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE a1 <> 2 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE a1 IN     (2,3) ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE a1 NOT IN (2,3) ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE a1 BETWEEN     2 AND 4 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE a1 NOT BETWEEN 2 AND 4 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE a1 IS NULL ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE a1 IS NOT NULL ORDER BY t0, a1;

-- test numeric predicates
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE c3 =  1.11 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE c3 <  1.11 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE c3 <= 1.11 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE c3 >  1.11 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE c3 >= 1.11 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE c3 <> 1.11 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE c3 IN     (1.11,2.21) ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE c3 NOT IN (1.11,2.21) ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE c3 BETWEEN     1.11 AND 4.41 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE c3 NOT BETWEEN 1.11 AND 4.41 ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE c3 IS NULL ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE c3 IS NOT NULL ORDER BY t0, a1;

-- test char predicates
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE d4 =  'BB' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE d4 =  'BB ' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE d4 <  'BB' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE d4 <= 'BB' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE d4 >  'BB' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE d4 >= 'BB' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE d4 <> 'BB' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE d4 LIKE     'B%' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE d4 NOT LIKE 'B%' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE d4 IN     ('BB','CC') ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE d4 NOT IN ('BB','CC') ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE d4 BETWEEN     'AA' AND 'CC' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE d4 NOT BETWEEN 'AA' AND 'CC' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE d4 IS NULL ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE d4 IS NOT NULL ORDER BY t0, a1;

-- test varchar predicates
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE e5 =  'BB' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE e5 =  'BB ' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE e5 <  'BB' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE e5 <= 'BB' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE e5 >  'BB' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE e5 >= 'BB' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE e5 <> 'BB' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE e5 LIKE     'B%' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE e5 NOT LIKE 'B%' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE e5 IN     ('BB','CC') ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE e5 NOT IN ('BB','CC') ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE e5 BETWEEN     'AA' AND 'CC' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE e5 NOT BETWEEN 'AA' AND 'CC' ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE e5 IS NULL ORDER BY t0, a1;
SELECT t0, a1, b2, c3, d4, e5, filterValue FROM test_filter WHERE e5 IS NOT NULL ORDER BY t0, a1;

-- test newly supported types
-- bpchar
SELECT x1, filterValue FROM test_filter WHERE x1 =  'BB' ORDER BY t0, a1;
SELECT x1, filterValue FROM test_filter WHERE x1 =  'BB ' ORDER BY t0, a1;
SELECT x1, filterValue FROM test_filter WHERE x1 <  'BC' ORDER BY t0, a1;
SELECT x1, filterValue FROM test_filter WHERE x1 <= 'BB' ORDER BY t0, a1;
SELECT x1, filterValue FROM test_filter WHERE x1 >  'BB' ORDER BY t0, a1;
SELECT x1, filterValue FROM test_filter WHERE x1 >= 'BB' ORDER BY t0, a1;
SELECT x1, filterValue FROM test_filter WHERE x1 <> 'BB' ORDER BY t0, a1;
SELECT x1, filterValue FROM test_filter WHERE x1 LIKE 'B%' ORDER BY t0, a1;
SELECT x1, filterValue FROM test_filter WHERE x1 NOT LIKE 'B%' ORDER BY t0, a1;
SELECT x1, filterValue FROM test_filter WHERE x1 IN ('BB','CC') ORDER BY t0, a1;
SELECT x1, filterValue FROM test_filter WHERE x1 NOT IN ('BB','CC') ORDER BY t0, a1;
SELECT x1, filterValue FROM test_filter WHERE x1 BETWEEN 'AA' AND 'CC' ORDER BY t0, a1;
SELECT x1, filterValue FROM test_filter WHERE x1 NOT BETWEEN 'AA' AND 'CC' ORDER BY t0, a1;
SELECT x1, filterValue FROM test_filter WHERE x1 IS NULL ORDER BY t0, a1;
SELECT x1, filterValue FROM test_filter WHERE x1 IS NOT NULL ORDER BY t0, a1;

-- smallint
SELECT x2, filterValue FROM test_filter WHERE x2 =  2::int2 ORDER BY t0, a1;
SELECT x2, filterValue FROM test_filter WHERE x2 <  2::int2 ORDER BY t0, a1;
SELECT x2, filterValue FROM test_filter WHERE x2 <= 2::int2 ORDER BY t0, a1;
SELECT x2, filterValue FROM test_filter WHERE x2 >  2::int2 ORDER BY t0, a1;
SELECT x2, filterValue FROM test_filter WHERE x2 >= 2::int2 ORDER BY t0, a1;
SELECT x2, filterValue FROM test_filter WHERE x2 <> 2::int2 ORDER BY t0, a1;
SELECT x2, filterValue FROM test_filter WHERE x2 IN (2::int,3::int2) ORDER BY t0, a1;
SELECT x2, filterValue FROM test_filter WHERE x2 NOT IN (2::int2,3::int2) ORDER BY t0, a1;
SELECT x2, filterValue FROM test_filter WHERE x2 BETWEEN 2::int2 AND 4::int2 ORDER BY t0, a1;
SELECT x2, filterValue FROM test_filter WHERE x2 NOT BETWEEN 2::int2 AND 4::int2 ORDER BY t0, a1;
SELECT x2, filterValue FROM test_filter WHERE x2 IS NULL ORDER BY t0, a1;
SELECT x2, filterValue FROM test_filter WHERE x2 IS NOT NULL ORDER BY t0, a1;

-- bigint
SELECT x3, filterValue FROM test_filter WHERE x3 =  1::int8 ORDER BY t0, a1;
SELECT x3, filterValue FROM test_filter WHERE x3 <  2::int8 ORDER BY t0, a1;
SELECT x3, filterValue FROM test_filter WHERE x3 <= 2::int8 ORDER BY t0, a1;
SELECT x3, filterValue FROM test_filter WHERE x3 >  2::int8 ORDER BY t0, a1;
SELECT x3, filterValue FROM test_filter WHERE x3 >= 2::int8 ORDER BY t0, a1;
SELECT x3, filterValue FROM test_filter WHERE x3 <> 2::int8 ORDER BY t0, a1;
SELECT x3, filterValue FROM test_filter WHERE x3 IN (2::int8,3::int8) ORDER BY t0, a1;
SELECT x3, filterValue FROM test_filter WHERE x3 NOT IN (2::int8,3::int8) ORDER BY t0, a1;
SELECT x3, filterValue FROM test_filter WHERE x3 BETWEEN 2::int8 AND 4::int8 ORDER BY t0, a1;
SELECT x3, filterValue FROM test_filter WHERE x3 NOT BETWEEN 2::int8 AND 4::int8 ORDER BY t0, a1;
SELECT x3, filterValue FROM test_filter WHERE x3 IS NULL ORDER BY t0, a1;
SELECT x3, filterValue FROM test_filter WHERE x3 IS NOT NULL ORDER BY t0, a1;

-- real
SELECT x4, filterValue FROM test_filter WHERE x4 =  1.11::real ORDER BY t0, a1;
SELECT x4, filterValue FROM test_filter WHERE x4 <  1.11::real ORDER BY t0, a1;
SELECT x4, filterValue FROM test_filter WHERE x4 <= 1.11::real ORDER BY t0, a1;
SELECT x4, filterValue FROM test_filter WHERE x4 >  1.11::real ORDER BY t0, a1;
SELECT x4, filterValue FROM test_filter WHERE x4 >= 1.11::real ORDER BY t0, a1;
SELECT x4, filterValue FROM test_filter WHERE x4 <> 1.11::real ORDER BY t0, a1;
SELECT x4, filterValue FROM test_filter WHERE x4 IN (1.11::real,2.21::real) ORDER BY t0, a1;
SELECT x4, filterValue FROM test_filter WHERE x4 NOT IN (1.11::real, 2.21::real) ORDER BY t0, a1;
SELECT x4, filterValue FROM test_filter WHERE x4 BETWEEN 1.11::real AND 4.41::real ORDER BY t0, a1;
SELECT x4, filterValue FROM test_filter WHERE x4 NOT BETWEEN 1.11::real AND 4.41::real ORDER BY t0, a1;
SELECT x4, filterValue FROM test_filter WHERE x4 IS NULL ORDER BY t0, a1;
SELECT x4, filterValue FROM test_filter WHERE x4 IS NOT NULL ORDER BY t0, a1;

-- float8
SELECT x5, filterValue FROM test_filter WHERE x5 =  1.11::float8 ORDER BY t0, a1;
SELECT x5, filterValue FROM test_filter WHERE x5 <  1.11::float8 ORDER BY t0, a1;
SELECT x5, filterValue FROM test_filter WHERE x5 <= 1.11::float8 ORDER BY t0, a1;
SELECT x5, filterValue FROM test_filter WHERE x5 >  1.11::float8 ORDER BY t0, a1;
SELECT x5, filterValue FROM test_filter WHERE x5 >= 1.11::float8 ORDER BY t0, a1;
SELECT x5, filterValue FROM test_filter WHERE x5 <> 1.11::float8 ORDER BY t0, a1;
SELECT x5, filterValue FROM test_filter WHERE x5 IN (1.11::float8, 2.21::float8) ORDER BY t0, a1;
SELECT x5, filterValue FROM test_filter WHERE x5 NOT IN (1.11::float8, 2.21::float8) ORDER BY t0, a1;
SELECT x5, filterValue FROM test_filter WHERE x5 BETWEEN 1.11::float8 AND 4.41::float8 ORDER BY t0, a1;
SELECT x5, filterValue FROM test_filter WHERE x5 NOT BETWEEN 1.11::float8 AND 4.41::float8 ORDER BY t0, a1;
SELECT x5, filterValue FROM test_filter WHERE x5 IS NULL ORDER BY t0, a1;
SELECT x5, filterValue FROM test_filter WHERE x5 IS NOT NULL ORDER BY t0, a1;

-- bytea
--start_ignore
set bytea_output = 'hex';
--end_ignore
SELECT x6, filterValue FROM test_filter WHERE x6 = '\132greenplum\132'::bytea ORDER BY t0, a1;
SELECT x6, filterValue FROM test_filter WHERE x6 < '\132greenplux\132'::bytea ORDER BY t0, a1;
SELECT x6, filterValue FROM test_filter WHERE x6 <= '\132greenplum\132'::bytea ORDER BY t0, a1;
SELECT x6, filterValue FROM test_filter WHERE x6 > '\132greenplum'::bytea ORDER BY t0, a1;
SELECT x6, filterValue FROM test_filter WHERE x6 >= '\132greenplum\132'::bytea ORDER BY t0, a1;
SELECT x6, filterValue FROM test_filter WHERE x6 <> '\132greeenplum\132'::bytea ORDER BY t0, a1;
SELECT x6, filterValue FROM test_filter WHERE x6 LIKE '\132gre%' ORDER BY t0, a1;
SELECT x6, filterValue FROM test_filter WHERE x6 NOT LIKE 'green%' ORDER BY t0, a1;
SELECT x6, filterValue FROM test_filter WHERE x6 IN ('\132greenplum\132'::bytea,'sdas\132'::bytea) ORDER BY t0, a1;
SELECT x6, filterValue FROM test_filter WHERE x6 NOT IN ('\132grenplum\132'::bytea,'sdas\132'::bytea) ORDER BY t0, a1;
SELECT x6, filterValue FROM test_filter WHERE x6 BETWEEN '\132greenplum\132'::bytea AND 'sdas\132'::bytea ORDER BY t0, a1;
SELECT x6, filterValue FROM test_filter WHERE x6 NOT BETWEEN '\132greenplup\132'::bytea AND 'sdas\132'::bytea ORDER BY t0, a1;
SELECT x6, filterValue FROM test_filter WHERE x6 IS NULL ORDER BY t0, a1;
SELECT x6, filterValue FROM test_filter WHERE x6 IS NOT NULL ORDER BY t0, a1;
--start_ignore
reset bytea_output;
--end_ignore

-- date
SELECT x7, filterValue FROM test_filter WHERE x7 = '2023-01-11'::date ORDER BY t0, a1;
SELECT x7, filterValue FROM test_filter WHERE x7 < '2023-01-12'::date ORDER BY t0, a1;
SELECT x7, filterValue FROM test_filter WHERE x7 <= '2023-01-12'::date ORDER BY t0, a1;
SELECT x7, filterValue FROM test_filter WHERE x7 > '2023-01-11'::date ORDER BY t0, a1;
SELECT x7, filterValue FROM test_filter WHERE x7 >= '2023-01-12'::date ORDER BY t0, a1;
SELECT x7, filterValue FROM test_filter WHERE x7 <> '2023-01-15'::date ORDER BY t0, a1;
SELECT x7, filterValue FROM test_filter WHERE x7 IN ('2023-01-12'::date,'2023-01-15'::date) ORDER BY t0, a1;
SELECT x7, filterValue FROM test_filter WHERE x7 NOT IN ('2023-01-15'::date,'2023-01-15'::date) ORDER BY t0, a1;
SELECT x7, filterValue FROM test_filter WHERE x7 BETWEEN '2023-01-11'::date AND '2023-01-19'::date ORDER BY t0, a1;
SELECT x7, filterValue FROM test_filter WHERE x7 NOT BETWEEN '2023-01-10'::date AND '2023-01-13'::date ORDER BY t0, a1;
SELECT x7, filterValue FROM test_filter WHERE x7 IS NULL ORDER BY t0, a1;
SELECT x7, filterValue FROM test_filter WHERE x7 IS NOT NULL ORDER BY t0, a1;

-- time
SELECT x8, filterValue FROM test_filter WHERE x8 = '12:34:50'::time ORDER BY t0, a1;
SELECT x8, filterValue FROM test_filter WHERE x8 < '12:34:52'::time ORDER BY t0, a1;
SELECT x8, filterValue FROM test_filter WHERE x8 <= '12:34:52'::time ORDER BY t0, a1;
SELECT x8, filterValue FROM test_filter WHERE x8 > '12:34:51'::time ORDER BY t0, a1;
SELECT x8, filterValue FROM test_filter WHERE x8 >= '12:34:52'::time ORDER BY t0, a1;
SELECT x8, filterValue FROM test_filter WHERE x8 <> '12:34:55'::time ORDER BY t0, a1;
SELECT x8, filterValue FROM test_filter WHERE x8 IN ('12:34:52'::time,'12:34:55'::time) ORDER BY t0, a1;
SELECT x8, filterValue FROM test_filter WHERE x8 NOT IN ('12:34:55'::time,'12:34:55'::time) ORDER BY t0, a1;
SELECT x8, filterValue FROM test_filter WHERE x8 BETWEEN '12:34:51'::time AND '12:34:59'::time ORDER BY t0, a1;
SELECT x8, filterValue FROM test_filter WHERE x8 NOT BETWEEN '12:34:51'::time AND '12:34:59'::time ORDER BY t0, a1;
SELECT x8, filterValue FROM test_filter WHERE x8 IS NULL ORDER BY t0, a1;
SELECT x8, filterValue FROM test_filter WHERE x8 IS NOT NULL ORDER BY t0, a1;

-- timestamp
SELECT x9, filterValue FROM test_filter WHERE x9 = '2023-01-01 12:34:50'::timestamp ORDER BY t0, a1;
SELECT x9, filterValue FROM test_filter WHERE x9 <  '2023-01-01 12:34:52'::timestamp ORDER BY t0, a1;
SELECT x9, filterValue FROM test_filter WHERE x9 <= '2023-01-01 12:34:52'::timestamp ORDER BY t0, a1;
SELECT x9, filterValue FROM test_filter WHERE x9 > '2023-01-01 12:34:51'::timestamp ORDER BY t0, a1;
SELECT x9, filterValue FROM test_filter WHERE x9 >= '2023-01-01 12:34:52'::timestamp ORDER BY t0, a1;
SELECT x9, filterValue FROM test_filter WHERE x9 <> '2023-01-01 12:34:55'::timestamp ORDER BY t0, a1;
SELECT x9, filterValue FROM test_filter WHERE x9 IN ('2023-01-01 12:34:52'::timestamp,'2023-01-01 12:34:55'::timestamp) ORDER BY t0, a1;
SELECT x9, filterValue FROM test_filter WHERE x9 NOT IN ('2023-01-01 12:34:55'::timestamp,'2023-01-01 12:34:55'::timestamp) ORDER BY t0, a1;
SELECT x9, filterValue FROM test_filter WHERE x9 BETWEEN '2023-01-01 12:34:51'::timestamp AND '2023-01-01 12:34:59'::timestamp ORDER BY t0, a1;
SELECT x9, filterValue FROM test_filter WHERE x9 NOT BETWEEN '2023-01-01 12:34:51'::timestamp AND '2023-01-01 12:34:59'::timestamp ORDER BY t0, a1;
SELECT x9, filterValue FROM test_filter WHERE x9 IS NULL ORDER BY t0, a1;
SELECT x9, filterValue FROM test_filter WHERE x9 IS NOT NULL ORDER BY t0, a1;

-- interval
SELECT x11, filterValue FROM test_filter WHERE x11 = '1 hour 30 minutes'::interval ORDER BY t0, a1;
SELECT x11, filterValue FROM test_filter WHERE x11 < '1 hour 35 minutes'::interval ORDER BY t0, a1;
SELECT x11, filterValue FROM test_filter WHERE x11 <= '1 hour 36 minutes'::interval ORDER BY t0, a1;
SELECT x11, filterValue FROM test_filter WHERE x11 > '1 hour 33 minutes'::interval ORDER BY t0, a1;
SELECT x11, filterValue FROM test_filter WHERE x11 >= '1 hour 35 minutes'::interval ORDER BY t0, a1;
SELECT x11, filterValue FROM test_filter WHERE x11 <> '1 hour 31 minutes'::interval ORDER BY t0, a1;
SELECT x11, filterValue FROM test_filter WHERE x11 IN ('1 hour 30 minutes'::interval,'1 hour 33 minutes'::interval) ORDER BY t0, a1;
SELECT x11, filterValue FROM test_filter WHERE x11 NOT IN ('1 hour 30 minutes'::interval,'1 hour 33 minutes'::interval) ORDER BY t0, a1;
SELECT x11, filterValue FROM test_filter WHERE x11 BETWEEN '1 hour 30 minutes'::interval AND '1 hour 35 minutes'::interval ORDER BY t0, a1;
SELECT x11, filterValue FROM test_filter WHERE x11 NOT BETWEEN '1 hour 30 minutes'::interval AND '1 hour 31 minutes'::interval ORDER BY t0, a1;
SELECT x11, filterValue FROM test_filter WHERE x11 IS NULL ORDER BY t0, a1;
SELECT x11, filterValue FROM test_filter WHERE x11 IS NOT NULL ORDER BY t0, a1;

-- uuid
SELECT x12, filterValue FROM test_filter WHERE x12 = '93d8f9c0-c314-447b-8690-60c40facb8a0'::uuid ORDER BY t0, a1;
SELECT x12, filterValue FROM test_filter WHERE x12 < '93d8f9c0-c314-447b-8690-60c40facb8a5'::uuid ORDER BY t0, a1;
SELECT x12, filterValue FROM test_filter WHERE x12 <= '93d8f9c0-c314-447b-8690-60c40facb8a5'::uuid ORDER BY t0, a1;
SELECT x12, filterValue FROM test_filter WHERE x12 > '93d8f9c0-c314-447b-8690-60c40facb8a2'::uuid ORDER BY t0, a1;
SELECT x12, filterValue FROM test_filter WHERE x12 >= '93d8f9c0-c314-447b-8690-60c40facb8a2'::uuid ORDER BY t0, a1;
SELECT x12, filterValue FROM test_filter WHERE x12 <> '93d8f9c0-c314-447b-8690-60c40facb8a2'::uuid ORDER BY t0, a1;
SELECT x12, filterValue FROM test_filter WHERE x12 IN ('93d8f9c0-c314-447b-8690-60c40facb8a0'::uuid,'93d8f9c0-c314-447b-8690-60c40facb8a2'::uuid) ORDER BY t0, a1;
SELECT x12, filterValue FROM test_filter WHERE x12 NOT IN ('93d8f9c0-c314-447b-8690-60c40facb8a2'::uuid ,'93d8f9c0-c314-447b-8690-60c40facb8a0'::uuid) ORDER BY t0, a1;
SELECT x12, filterValue FROM test_filter WHERE x12 BETWEEN '93d8f9c0-c314-447b-8690-60c40facb8a0'::uuid AND '93d8f9c0-c314-447b-8690-60c40facb8a4'::uuid ORDER BY t0, a1;
SELECT x12, filterValue FROM test_filter WHERE x12 NOT BETWEEN '93d8f9c0-c314-447b-8690-60c40facb8a0'::uuid AND '93d8f9c0-c314-447b-8690-60c40facb8a2'::uuid ORDER BY t0, a1;
SELECT x12, filterValue FROM test_filter WHERE x12 IS NULL ORDER BY t0, a1;
SELECT x12, filterValue FROM test_filter WHERE x12 IS NOT NULL ORDER BY t0, a1;

-- json
SELECT x13, filterValue FROM test_filter WHERE x13 IS NULL ORDER BY t0, a1;
SELECT x13, filterValue FROM test_filter WHERE x13 IS NOT NULL ORDER BY t0, a1;

-- jsonb
SELECT x14, filterValue FROM test_filter WHERE x14 = '{"a":0}' ::jsonb ORDER BY t0, a1;
SELECT x14, filterValue FROM test_filter WHERE x14 <  '{"a":3}' ::jsonb ORDER BY t0, a1;
SELECT x14, filterValue FROM test_filter WHERE x14 <= '{"a":3}' ::jsonb  ORDER BY t0, a1;
SELECT x14, filterValue FROM test_filter WHERE x14 > '{"a":1}' ::jsonb ORDER BY t0, a1;
SELECT x14, filterValue FROM test_filter WHERE x14 >= '{"a":4}' ::jsonb ORDER BY t0, a1;
SELECT x14, filterValue FROM test_filter WHERE x14 <> '{"a":5}' ::jsonb ORDER BY t0, a1;
SELECT x14, filterValue FROM test_filter WHERE x14 IN  ('{"a":6}' ::jsonb,'{"a":1}' ::jsonb) ORDER BY t0, a1;
SELECT x14, filterValue FROM test_filter WHERE x14 NOT IN ('{"a":8}' ::jsonb,'{"a":4}' ::jsonb) ORDER BY t0, a1;
SELECT x14, filterValue FROM test_filter WHERE x14 BETWEEN '{"a":1}' ::jsonb AND '{"a":4}' ::jsonb ORDER BY t0, a1;
SELECT x14, filterValue FROM test_filter WHERE x14 NOT BETWEEN '{"a":1}' ::jsonb AND '{"a":4}' ::jsonb ORDER BY t0, a1;
SELECT x14, filterValue FROM test_filter WHERE x14 IS NULL ORDER BY t0, a1;
SELECT x14, filterValue FROM test_filter WHERE x14 IS NOT NULL ORDER BY t0, a1;

-- int2 array
SELECT x15, filterValue FROM test_filter WHERE x15 = array[0::int2, 1::int2] ORDER BY t0, a1;
SELECT x15, filterValue FROM test_filter WHERE x15 <> array[0::int2, 1::int2, null] ORDER BY t0, a1;
SELECT x15, filterValue FROM test_filter WHERE x15 IS NULL ORDER BY t0, a1;
SELECT x15, filterValue FROM test_filter WHERE x15 IS NOT NULL ORDER BY t0, a1;
SELECT x15, filterValue FROM test_filter WHERE x15 IN (array[1::int2, 2::int2], array[5::int2, 6::int2]);
SELECT x15, filterValue FROM test_filter WHERE x15 NOT IN (array[4::int2, 5::int2], array[5::int2, 6::int2]);

-- int4 array
SELECT x16, filterValue FROM test_filter WHERE x16 = array[2::int4, 3::int4] ORDER BY t0, a1;
SELECT x16, filterValue FROM test_filter WHERE x16 <> array[0::int4, 1::int4, null] ORDER BY t0, a1;
SELECT x16, filterValue FROM test_filter WHERE x16 IS NULL ORDER BY t0, a1;
SELECT x16, filterValue FROM test_filter WHERE x16 IS NOT NULL ORDER BY t0, a1;
SELECT x16, filterValue FROM test_filter WHERE x16 IN (array[2::int4, 3::int4], array[5::int4, 6::int4]);
SELECT x16, filterValue FROM test_filter WHERE x16 NOT IN (array[4::int4, 5::int4], array[5::int4, 6::int4]);

--int8array
SELECT x17, filterValue FROM test_filter WHERE x17 = array[4::bigint, 5::bigint] ORDER BY t0, a1;
SELECT x17, filterValue FROM test_filter WHERE x17 <> array[5::bigint, 4::bigint, null] ORDER BY t0, a1;
SELECT x17, filterValue FROM test_filter WHERE x17 IS NULL ORDER BY t0, a1;
SELECT x17, filterValue FROM test_filter WHERE x17 IS NOT NULL ORDER BY t0, a1;
SELECT x17, filterValue FROM test_filter WHERE x17 IN (array[4::bigint, 5::bigint], array[5::bigint, 6::bigint]);
SELECT x17, filterValue FROM test_filter WHERE x17 NOT IN (array[4::bigint, 5::bigint], array[5::bigint, 6::bigint]);

-- bool array
SELECT x18, filterValue FROM test_filter WHERE x18 = array[true, false, null] ORDER BY t0, a1;
SELECT x18, filterValue FROM test_filter WHERE x18 <> array[true, false, null] ORDER BY t0, a1;
SELECT x18, filterValue FROM test_filter WHERE x18 IS NULL ORDER BY t0, a1;
SELECT x18, filterValue FROM test_filter WHERE x18 IS NOT NULL ORDER BY t0, a1;
SELECT x18, filterValue FROM test_filter WHERE x18 IN (array[true, false, null], array[true]);
SELECT x18, filterValue FROM test_filter WHERE x18 NOT IN (array[true, false, null], array[true]);

-- text array
SELECT x19, filterValue FROM test_filter WHERE x19 = array['B'::text, 'B'::text] ORDER BY t0, a1;
SELECT x19, filterValue FROM test_filter WHERE x19 <> array['A'::text, 'A'::text, null] ORDER BY t0, a1;
SELECT x19, filterValue FROM test_filter WHERE x19 IS NULL ORDER BY t0, a1;
SELECT x19, filterValue FROM test_filter WHERE x19 IS NOT NULL ORDER BY t0, a1;
SELECT x19, filterValue FROM test_filter WHERE x19 IN (array['B'::text, 'B'::text], array['A'::text, 'A'::text, null]);
SELECT x19, filterValue FROM test_filter WHERE x19 NOT IN (array['A'::text, 'A'::text], array['A'::text, 'A'::text, null]);

-- float4 array
SELECT x20, filterValue FROM test_filter WHERE x20 = array[1.1::float4, 2.1::float4] ORDER BY t0, a1;
SELECT x20, filterValue FROM test_filter WHERE x20 <> array[0::float4, 1::float4, null] ORDER BY t0, a1;
SELECT x20, filterValue FROM test_filter WHERE x20 IS NULL ORDER BY t0, a1;
SELECT x20, filterValue FROM test_filter WHERE x20 IS NOT NULL ORDER BY t0, a1;
SELECT x20, filterValue FROM test_filter WHERE x20 IN (array[1.1::float4, 2.1::float4], array[5::float4, 6::float4]);
SELECT x20, filterValue FROM test_filter WHERE x20 NOT IN (array[4::float4, 5::float4], array[5::float4, 6::float4]);

-- float8 array
SELECT x21, filterValue FROM test_filter WHERE x21 = array[1.1::float8, 2.1::float8] ORDER BY t0, a1;
SELECT x21, filterValue FROM test_filter WHERE x21 <> array[0::float8, 1::float8, null] ORDER BY t0, a1;
SELECT x21, filterValue FROM test_filter WHERE x21 IS NULL ORDER BY t0, a1;
SELECT x21, filterValue FROM test_filter WHERE x21 IS NOT NULL ORDER BY t0, a1;
SELECT x21, filterValue FROM test_filter WHERE x21 IN (array[1.1::float8, 2.1::float8], array[5::float8, 6::float8]);
SELECT x21, filterValue FROM test_filter WHERE x21 NOT IN (array[4::float8, 5::float8], array[5::float8, 6::float8]);

-- bytea array
--start_ignore
set bytea_output = 'hex';
--end_ignore
SELECT x22, filterValue FROM test_filter WHERE x22 = array['\x78343142'::bytea,'\x78343242'::bytea] ORDER BY t0, a1;
SELECT x22, filterValue FROM test_filter WHERE x22 <> array['\132greenplum\132'::bytea,'sdas\132'::bytea, null] ORDER BY t0, a1;
SELECT x22, filterValue FROM test_filter WHERE x22 IS NULL ORDER BY t0, a1;
SELECT x22, filterValue FROM test_filter WHERE x22 IS NOT NULL ORDER BY t0, a1;
SELECT x22, filterValue FROM test_filter WHERE x22 IN (array['\x78343142'::bytea,'\x78343242'::bytea], array['sdas\132'::bytea]);
SELECT x22, filterValue FROM test_filter WHERE x22 NOT IN (array['\132greenplum\132'::bytea,'sdas\132'::bytea, null], array['sdas\132'::bytea]);
--start_ignore
reset bytea_output;
--end_ignore

-- bpchar array
SELECT x23, filterValue FROM test_filter WHERE x23 = array['AA'::bpchar(2), 'AA'::bpchar(2)] ORDER BY t0, a1;
SELECT x23, filterValue FROM test_filter WHERE x23 <> array['AB'::bpchar(2), 'AB'::bpchar(2), null] ORDER BY t0, a1;
SELECT x23, filterValue FROM test_filter WHERE x23 IS NULL ORDER BY t0, a1;
SELECT x23, filterValue FROM test_filter WHERE x23 IS NOT NULL ORDER BY t0, a1;
SELECT x23, filterValue FROM test_filter WHERE x23 IN (array['AA'::bpchar(2), 'AA'::bpchar(2)], array['AA'::bpchar(2), 'AB'::bpchar(2), null]);
SELECT x23, filterValue FROM test_filter WHERE x23 NOT IN (array['A'::bpchar(2), 'A'::bpchar(2)], array['A'::bpchar(2), 'A'::bpchar(2), null]);

-- varchar array
SELECT x24, filterValue FROM test_filter WHERE x24 = array['AA'::varchar(2), '66'::varchar(2)] ORDER BY t0, a1;
SELECT x24, filterValue FROM test_filter WHERE x24 <> array['BB'::varchar(2), 'C'::varchar(2), null] ORDER BY t0, a1;
SELECT x24, filterValue FROM test_filter WHERE x24 IS NULL ORDER BY t0, a1;
SELECT x24, filterValue FROM test_filter WHERE x24 IS NOT NULL ORDER BY t0, a1;
SELECT x24, filterValue FROM test_filter WHERE x24 IN (array['AA'::varchar(2), '66'::varchar(2)], array['BB'::varchar(2), 'B'::varchar(2), null]);
SELECT x24, filterValue FROM test_filter WHERE x24 NOT IN (array['A'::varchar(2), 'A'::varchar(2)], array['A'::varchar(2), 'A'::varchar(2), null]);

-- date array
SELECT x25, filterValue FROM test_filter WHERE x25 = array['2023-01-01' ::date, '2023-01-02' ::date] ORDER BY t0, a1;
SELECT x25, filterValue FROM test_filter WHERE x25 <> array['2023-01-01' ::date, '2023-01-02' ::date, null] ORDER BY t0, a1;
SELECT x25, filterValue FROM test_filter WHERE x25 IS NULL ORDER BY t0, a1;
SELECT x25, filterValue FROM test_filter WHERE x25 IS NOT NULL ORDER BY t0, a1;
SELECT x25, filterValue FROM test_filter WHERE x25 IN (array['2023-01-01' ::date, '2023-01-02' ::date], array['2023-01-01' ::date, '2023-01-02' ::date, null]);
SELECT x25, filterValue FROM test_filter WHERE x25 NOT IN (array['2023-01-01' ::date, '2023-01-03' ::date, null], array['2023-01-01' ::date, '2023-01-03' ::date]);

-- uuid array
SELECT x26, filterValue FROM test_filter WHERE x26 = array['93d8f9c0-c314-447b-8690-60c40facb8a5'::uuid, 'a56bc0c8-2128-4269-9ce5-cd9c102227b0'::uuid] ORDER BY t0, a1;
SELECT x26, filterValue FROM test_filter WHERE x26 <> array['93d8f9c0-c314-447b-8690-60d40facb8a5'::uuid, '93d8f9c0-c315-447b-8690-60c40facb8a5'::uuid] ORDER BY t0, a1;
SELECT x26, filterValue FROM test_filter WHERE x26 IS NULL ORDER BY t0, a1;
SELECT x26, filterValue FROM test_filter WHERE x26 IS NOT NULL ORDER BY t0, a1;
SELECT x26, filterValue FROM test_filter WHERE x26 IN (array['93d8f9c0-c314-447b-8690-60c40facb8a5'::uuid, 'a56bc0c8-2128-4269-9ce5-cd9c102227b0'::uuid], array['93d8f9c0-c314-447b-8690-60c40facb8a5'::uuid, '93d8f9c0-c314-447b-8690-60c40facb8a5'::uuid, null]);
SELECT x26, filterValue FROM test_filter WHERE x26 NOT IN (array['93d8f9c0-c314-447b-8690-60c40facb8a5'::uuid, '93d8f9c0-c314-447b-8690-60c40facb8a5'::uuid, null], array['93d8f9c1-c314-447b-8690-60c40facb8a5'::uuid, '93d8f9c0-c314-447b-8690-60c40facb8a5'::uuid, null]);

-- numeric array
SELECT x27, filterValue FROM test_filter WHERE x27 = array[1.1::numeric, 2.1::numeric] ORDER BY t0, a1;
SELECT x27, filterValue FROM test_filter WHERE x27 <> array[0::numeric, 1::numeric, null] ORDER BY t0, a1;
SELECT x27, filterValue FROM test_filter WHERE x27 IS NULL ORDER BY t0, a1;
SELECT x27, filterValue FROM test_filter WHERE x27 IS NOT NULL ORDER BY t0, a1;
SELECT x27, filterValue FROM test_filter WHERE x27 IN (array[1.1::numeric, 2.1::numeric], array[5::numeric, 6::numeric]);
SELECT x27, filterValue FROM test_filter WHERE x27 NOT IN (array[4::numeric, 5::numeric], array[5::numeric, 6::numeric]);

-- time array
SELECT x28, filterValue FROM test_filter WHERE x28 = array['12:00:00' ::time, '13:00:00' ::time] ORDER BY t0, a1;
SELECT x28, filterValue FROM test_filter WHERE x28 <> array['12:00:00' ::time, '13:00:00' ::time, null] ORDER BY t0, a1;
SELECT x28, filterValue FROM test_filter WHERE x28 IS NULL ORDER BY t0, a1;
SELECT x28, filterValue FROM test_filter WHERE x28 IS NOT NULL ORDER BY t0, a1;
SELECT x28, filterValue FROM test_filter WHERE x28 IN (array['12:00:00' ::time, '13:00:00' ::time], array['12:00:00' ::time, '13:00:00' ::time, null]);
SELECT x28, filterValue FROM test_filter WHERE x28 NOT IN (array['12:00:02' ::time, '13:00:00' ::time], array['12:00:00' ::time, '13:04:00' ::time]);

-- timestamp  array
SELECT x29, filterValue FROM test_filter WHERE x29 = array['2023-01-01 12:00:00' ::timestamp, '2023-01-02 12:00:00' ::timestamp] ORDER BY t0, a1;
SELECT x29, filterValue FROM test_filter WHERE x29 <> array['2023-01-02 12:00:00' ::timestamp, '2023-01-02 12:00:00' ::timestamp, null] ORDER BY t0, a1;
SELECT x29, filterValue FROM test_filter WHERE x29 IS NULL ORDER BY t0, a1;
SELECT x29, filterValue FROM test_filter WHERE x29 IS NOT NULL ORDER BY t0, a1;
SELECT x29, filterValue FROM test_filter WHERE x29 IN (array['2023-01-01 12:00:00' ::timestamp, '2023-01-02 12:00:00' ::timestamp], array['2023-01-01 12:00:00' ::timestamp, '2023-01-02 12:00:00' ::timestamp, null]);
SELECT x29, filterValue FROM test_filter WHERE x29 NOT IN (array['2023-01-03 12:00:00' ::timestamp, '2023-01-02 12:00:00' ::timestamp], array['2023-01-02 12:00:00' ::timestamp, '2023-01-02 12:00:00' ::timestamp]);

-- interval array
SELECT x31, filterValue FROM test_filter WHERE x31 = array['1 hour' ::interval, '2 hours' ::interval] ORDER BY t0, a1;
SELECT x31, filterValue FROM test_filter WHERE x31 <> array['1 hour' ::interval, '2 hours' ::interval, null] ORDER BY t0, a1;
SELECT x31, filterValue FROM test_filter WHERE x31 IS NULL ORDER BY t0, a1;
SELECT x31, filterValue FROM test_filter WHERE x31 IS NOT NULL ORDER BY t0, a1;
SELECT x31, filterValue FROM test_filter WHERE x31 IN (array['1 hour' ::interval, '2 hours' ::interval], array['1 hour' ::interval, '2 hours' ::interval, null]);
SELECT x31, filterValue FROM test_filter WHERE x31 NOT IN (array['1 hour' ::interval, '2 hours' ::interval, null], array['12 hours' ::interval, '2 hours' ::interval]);

-- json array
SELECT x32, filterValue FROM test_filter WHERE x32 IS NULL ORDER BY t0, a1;
SELECT x32, filterValue FROM test_filter WHERE x32 IS NOT NULL ORDER BY t0, a1;

-- jsonb array
SELECT x33, filterValue FROM test_filter WHERE x33 = array['{"a":0}'::jsonb] ORDER BY t0, a1;
SELECT x33, filterValue FROM test_filter WHERE x33 <> array['{"a":2}'::jsonb, null] ORDER BY t0, a1;
SELECT x33, filterValue FROM test_filter WHERE x33 IS NULL ORDER BY t0, a1;
SELECT x33, filterValue FROM test_filter WHERE x33 IS NOT NULL ORDER BY t0, a1;
SELECT x33, filterValue FROM test_filter WHERE x33 IN (array['{"a":2}'::jsonb], array['{"a":0}'::jsonb]);
SELECT x33, filterValue FROM test_filter WHERE x33 NOT IN (array['{"a":2}'::jsonb], array['{"a":0}'::jsonb]);
