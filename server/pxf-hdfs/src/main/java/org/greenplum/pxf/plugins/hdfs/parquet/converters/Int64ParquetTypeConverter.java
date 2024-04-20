package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetTimestampUtilities;

import java.math.BigDecimal;
import java.math.BigInteger;

public class Int64ParquetTypeConverter implements ParquetTypeConverter {
    private final boolean useLocalPxfTimezoneRead;

    public Int64ParquetTypeConverter(boolean useLocalPxfTimezoneRead) {
        this.useLocalPxfTimezoneRead = useLocalPxfTimezoneRead;
    }

    @Override
    public DataType getDataType(Type type) {
        if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            return DataType.NUMERIC;
        } else if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            return DataType.TIMESTAMP;
        }
        return DataType.BIGINT;
    }

    @Override
    public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
        long value = group.getLong(columnIndex, repeatIndex);
        if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            return bigDecimalFromLong((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation(), value);
        }
        if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            LogicalTypeAnnotation.TimestampLogicalTypeAnnotation originalType = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
            return ParquetTimestampUtilities.getTimestampFromLong(value, originalType.getUnit(), useLocalPxfTimezoneRead);
        }
        return value;
    }

    @Override
    public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
        if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            String timestamp = (String) getValue(group, columnIndex, repeatIndex, type);
            jsonNode.add(timestamp);
        } else {
            jsonNode.add(group.getLong(columnIndex, repeatIndex));
        }
    }

    @Override
    public String getValueFromList(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType) {
        return String.valueOf(getValue(group, columnIndex, repeatIndex, primitiveType));
    }

    private BigDecimal bigDecimalFromLong(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType, long value) {
        return new BigDecimal(BigInteger.valueOf(value), decimalType.getScale());
    }
}
