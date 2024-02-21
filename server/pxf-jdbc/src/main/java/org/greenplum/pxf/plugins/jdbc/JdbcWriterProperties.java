package org.greenplum.pxf.plugins.jdbc;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class JdbcWriterProperties {
    private final JdbcBasePlugin plugin;
    private final Integer batchSize;
    private final int batchTimeout;
    private final String query;
    private final int poolSize;
    private final int terminationTimeoutSeconds;
}
