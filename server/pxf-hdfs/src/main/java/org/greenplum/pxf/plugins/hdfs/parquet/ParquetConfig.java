package org.greenplum.pxf.plugins.hdfs.parquet;

public class ParquetConfig {
    private boolean useLocalPxfTimezoneRead;

    public boolean isUseLocalPxfTimezoneRead() {
        return useLocalPxfTimezoneRead;
    }

    public void setUseLocalPxfTimezoneRead(boolean useLocalPxfTimezoneRead) {
        this.useLocalPxfTimezoneRead = useLocalPxfTimezoneRead;
    }
}
