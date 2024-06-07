package org.greenplum.pxf.plugins.hdfs.parquet;

import lombok.Builder;
import lombok.Data;
import org.greenplum.pxf.plugins.hdfs.utilities.DecimalUtilities;

@Data
@Builder
public class ParquetConfig {
    private final boolean useLocalPxfTimezoneWrite;
    private final boolean useLocalPxfTimezoneRead;
    private final DecimalUtilities decimalUtilities;
}
