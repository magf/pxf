package org.greenplum.pxf.plugins.hdfs.parquet;

/**
 * Parquet types
 */
public enum PxfParquetType {
    BINARY,
    INT32,
    INT64,
    DOUBLE,
    INT96,
    FLOAT,
    FIXED_LEN_BYTE_ARRAY,
    BOOLEAN,
    LIST,
    CUSTOM
}
