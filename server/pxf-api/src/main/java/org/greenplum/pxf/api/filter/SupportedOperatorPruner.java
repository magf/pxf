package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;

import java.util.EnumSet;

/**
 * A tree pruner that removes operator nodes for non-supported operators.
 */
public class SupportedOperatorPruner extends BaseTreePruner {

    private final EnumSet<Operator> supportedOperators;
    private final EnumSet<DataType> supportedTypes;

    /**
     * Constructor
     *
     * @param supportedOperators the set of supported operators
     */
    public SupportedOperatorPruner(EnumSet<Operator> supportedOperators) {
        this(supportedOperators, null);
    }

    /**
     * Constructor
     *
     * @param supportedOperators the set of supported operators
     * @param supportedTypes
     */
    public SupportedOperatorPruner(EnumSet<Operator> supportedOperators, EnumSet<DataType> supportedTypes) {
        this.supportedOperators = supportedOperators;
        this.supportedTypes = supportedTypes;
    }

    @Override
    public Node visit(Node node, final int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            OperandNode data = operatorNode.getValueOperand();
            if (!supportedOperators.contains(operator)) {
                // prune the operator node if its operator is not supported
                LOG.debug("Operator {} is not supported", operator);
                return null;
            }
            if (supportedTypes != null && !supportedTypes.contains(data.getDataType())) {
                // prune type node if it's not supported
                LOG.debug("Type {} is not supported", data.getDataType());
                return null;
            }
        }
        return node;
    }
}
