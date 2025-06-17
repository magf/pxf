#!/usr/bin/env bash

# Check namenode
curl -f http://hadoop:9870/ || exit 1

# Check datanode
curl -f http://hadoop:9864/ || exit 1

# Check resource manager
curl -f http://hadoop:8088/ || exit 1

# Check node manager
curl -f http://hadoop:8042/ || exit 1

# Check hive
netstat -an | grep 10000 > /dev/null; if [ 0 != $? ]; then exit 1; fi;

# Check hbase
curl -f http://hadoop:16010/ || exit 1
