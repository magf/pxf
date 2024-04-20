package org.greenplum.pxf.plugins.hdfs.parquet;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ParquetConfig {
    private final boolean useLocalPxfTimezoneWrite;
    private final boolean useLocalPxfTimezoneRead;
}
