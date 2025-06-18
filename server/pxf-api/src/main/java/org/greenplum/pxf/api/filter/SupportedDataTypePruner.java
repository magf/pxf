package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.EnumSet;
import java.util.List;

/**
 * A tree pruner that removes operator nodes for non-supported Greengage column data types.
 */
public class SupportedDataTypePruner extends BaseTreePruner {

    private final List<ColumnDescriptor> columnDescriptors;
    private final EnumSet<DataType> supportedDataTypes;

    /**
     * Constructor
     *
     * @param columnDescriptors  the list of column descriptors for the table
     * @param supportedDataTypes the EnumSet of supported data types
     */
    public SupportedDataTypePruner(List<ColumnDescriptor> columnDescriptors,
                                   EnumSet<DataType> supportedDataTypes) {
        this.columnDescriptors = columnDescriptors;
        this.supportedDataTypes = supportedDataTypes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node visit(Node node, int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            if (!operatorNode.getOperator().isLogical()) {
                ColumnDescriptor columnDescriptor = columnDescriptors.get(operatorNode.getColumnIndexOperand().index());
                DataType datatype = columnDescriptor.getDataType();
                if (!supportedDataTypes.contains(datatype)) {
                    // prune the operator node if its operand is a column of unsupported type
                    LOG.debug("DataType (name={} oid={}) for column=(index:{} name:{}) is not supported",
                            datatype.name(), datatype.getOID(), columnDescriptor.columnIndex(), columnDescriptor.columnName());
                    return null;
                }
            }
        }
        return node;
    }
}
