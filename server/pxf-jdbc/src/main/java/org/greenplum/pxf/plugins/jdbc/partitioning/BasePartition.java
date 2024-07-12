package org.greenplum.pxf.plugins.jdbc.partitioning;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * A base class for partition of any type.
 * <p>
 * All partitions use some column as a partition column. It is processed by this class.
 */
@Getter
@RequiredArgsConstructor
public abstract class BasePartition implements JdbcFragmentMetadata {

    /**
     * Column name to use as a partition column. Must not be null
     */
    @NonNull
    protected final String column;

    protected String getQuotedColumn(String quoteString) {
        if (quoteString == null) {
            throw new RuntimeException("Quote string cannot be null");
        }
        return quoteString + column + quoteString;
    }
}
