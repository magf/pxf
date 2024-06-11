package org.greenplum.pxf.plugins.jdbc.partitioning;

import lombok.NonNull;

/**
 * A base class for partition of any type.
 * <p>
 * All partitions use some column as a partition column. It is processed by this class.
 */
public abstract class BaseRangePartition extends BasePartition {
    public BaseRangePartition(@NonNull String column) {
        super(column);
    }

    /**
     * Generate a range-based SQL constraint
     *
     * @param quotedColumn column name (used as is, thus it should be quoted if necessary)
     * @param start        range start to base constraint on
     * @param end          range end to base constraint on
     * @return a pure SQL constraint (without WHERE)
     */
    String generateRangeConstraint(String quotedColumn, String start, String end) {
        StringBuilder sb = new StringBuilder(quotedColumn);

        if (start == null) {
            sb.append(" < ").append(end);
        } else if (end == null) {
            sb.append(" >= ").append(start);
        } else {
            sb.append(" >= ").append(start)
                    .append(" AND ")
                    .append(quotedColumn).append(" < ").append(end);
        }

        return sb.toString();
    }
}
