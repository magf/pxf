package org.greenplum.pxf.plugins.hive;

import lombok.Getter;

import java.util.List;
import java.util.Properties;

/**
 * HiveAccessor parses information during initialization, this metadata is then
 * shared with HiveResolver.
 */
@Getter
public class HiveMetadata {
    private final Properties properties;
    private final List<HivePartition> partitions;
    private final List<Integer> hiveIndexes;

    public HiveMetadata(Properties properties, List<HivePartition> partitions, List<Integer> hiveIndexes) {
        this.properties = properties;
        this.partitions = partitions;
        this.hiveIndexes = hiveIndexes;
    }

}
