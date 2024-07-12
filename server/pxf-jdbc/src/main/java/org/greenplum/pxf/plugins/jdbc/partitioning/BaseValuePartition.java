package org.greenplum.pxf.plugins.jdbc.partitioning;

import lombok.NonNull;

/**
 * A base class for partition of any type.
 * <p>
 * All partitions use some column as a partition column. It is processed by this class.
 */
public abstract class BaseValuePartition extends BasePartition {
    public BaseValuePartition(@NonNull String column) {
        super(column);
    }

    /**
     * Generate a range-based SQL constraint
     *
     * @param quotedColumn column name (used as is, thus it should be quoted if necessary)
     * @param value        value to base constraint on
     * @return a pure SQL constraint (without WHERE)
     */
    String generateConstraint(String quotedColumn, String value) {
        return quotedColumn + " = " + value;
    }
}
