-- @description query01 for PXF test to check fragments distribution across segments with HDFS profile
SELECT * FROM fragment_distribution_hdfs_limit_ext_table ORDER BY 1 LIMIT 5;
