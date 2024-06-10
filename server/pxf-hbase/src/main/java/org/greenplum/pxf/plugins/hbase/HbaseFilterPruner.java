package org.greenplum.pxf.plugins.hbase;

import lombok.extern.slf4j.Slf4j;
import org.greenplum.pxf.api.filter.*;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseTupleDescription;

import java.util.EnumSet;
import java.util.List;

@Slf4j
public class HbaseFilterPruner extends SupportedOperatorPruner {
    private final EnumSet<DataType> supportedTypes;
    private final HBaseTupleDescription tupleDescription;
    /**
     * Constructor
     *
     * @param tupleDescription fields description
     * @param supportedTypes the set of supported data types
     * @param supportedOperators the set of supported operators
     */
    public HbaseFilterPruner(HBaseTupleDescription tupleDescription, EnumSet<DataType> supportedTypes, EnumSet<Operator> supportedOperators) {
        super(supportedOperators);
        this.supportedTypes = supportedTypes;
        this.tupleDescription = tupleDescription;
    }

    @Override
    public Node visit(Node node, int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            ColumnIndexOperandNode columnIndexOperand = operatorNode.getColumnIndexOperand();
            ColumnDescriptor columnDescriptor = tupleDescription.getColumn(columnIndexOperand.index());
            if (!operator.isLogical()
                    && !supportedTypes.contains(columnDescriptor.getDataType())) {
                log.debug("Type {} for column {} is not supported for filtering", columnDescriptor.getDataType(),
                        columnDescriptor.columnName());
                return null;
            }
        }
        return super.visit(node, level);
    }
}
