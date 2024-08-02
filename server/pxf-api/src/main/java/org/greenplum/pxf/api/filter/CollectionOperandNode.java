package org.greenplum.pxf.api.filter;

import lombok.Getter;
import org.greenplum.pxf.api.io.DataType;

import java.util.List;

/**
 * Represents a collection of values
 */
@Getter
public class CollectionOperandNode extends OperandNode {

    private final List<String> data;

    /**
     * Constructs a CollectionOperandNode with the given data type and a data
     * list
     *
     * @param dataType the data type
     * @param data     the data list
     */
    public CollectionOperandNode(DataType dataType, List<String> data) {
        super(dataType);
        this.data = data;
    }

    @Override
    public String toString() {
        return String.format("(%s)", String.join(",", data));
    }
}
