package org.greenplum.pxf.plugins.jdbc;

import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.SupportedOperatorPruner;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.EnumSet;
import java.util.List;

public class JdbcFilterPruner extends SupportedOperatorPruner {
    private final EnumSet<DataType> supportedTypes;
    private final List<ColumnDescriptor> tupleDescription;
    /**
     * Constructor
     *
     * @param tupleDescription fields description
     * @param supportedTypes the set of supported data types
     * @param supportedOperators the set of supported operators
     */
    public JdbcFilterPruner(List<ColumnDescriptor> tupleDescription, EnumSet<DataType> supportedTypes, EnumSet<Operator> supportedOperators) {
        super(supportedOperators, supportedTypes);
        this.supportedTypes = supportedTypes;
        this.tupleDescription = tupleDescription;
    }
}
