package org.greenplum.pxf.api.filter;

import lombok.Getter;
import org.greenplum.pxf.api.io.DataType;

/**
 * Scalar, Column Index, List
 */
@Getter
public class OperandNode extends Node {
    private final DataType dataType;

    /**
     * Constructs an OperandNode with the given data type
     *
     * @param dataType the data type
     */
    public OperandNode(DataType dataType) {
        this.dataType = dataType;
    }

}
