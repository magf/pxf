#!/bin/bash
# Health check for Hadoop services

# Check HDFS
curl -f http://localhost:9870/ > /dev/null 2>&1
hdfs_status=$?

# Check Hive
curl -f http://localhost:10000/ > /dev/null 2>&1
hive_status=$?

if [ $hdfs_status -eq 0 ] && [ $hive_status -eq 0 ]; then
    exit 0
else
    exit 1
fi
