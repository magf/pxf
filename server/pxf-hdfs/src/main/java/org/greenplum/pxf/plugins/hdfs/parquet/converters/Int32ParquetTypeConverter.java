package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.io.DataType;

import java.math.BigDecimal;
import java.math.BigInteger;

public class Int32ParquetTypeConverter implements ParquetTypeConverter {

    @Override
    public DataType getDataType(Type type) {
        LogicalTypeAnnotation originalType = type.getLogicalTypeAnnotation();
        if (originalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return DataType.DATE;
        } else if (originalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            return DataType.NUMERIC;
        } else if (originalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
            LogicalTypeAnnotation.IntLogicalTypeAnnotation intType = (LogicalTypeAnnotation.IntLogicalTypeAnnotation) originalType;
            if (intType.getBitWidth() == 8 || intType.getBitWidth() == 16) {
                return DataType.SMALLINT;
            }
        }
        return DataType.INTEGER;
    }

    @Override
    @SuppressWarnings("deprecation")
    public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
        int result = group.getInteger(columnIndex, repeatIndex);
        LogicalTypeAnnotation originalType = type.getLogicalTypeAnnotation();
        if (originalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return new org.apache.hadoop.hive.serde2.io.DateWritable(result).get(true);
        } else if (originalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            return bigDecimalFromLong((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) originalType, result);
        } else if (originalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
            LogicalTypeAnnotation.IntLogicalTypeAnnotation intType = (LogicalTypeAnnotation.IntLogicalTypeAnnotation) originalType;
            if (intType.getBitWidth() == 8 || intType.getBitWidth() == 16) {
                return (short) result;
            }
        }
        return result;
    }

    @Override
    public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
        jsonNode.add(group.getInteger(columnIndex, repeatIndex));
    }

    @Override
    public String getValueFromList(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType) {
        return String.valueOf(getValue(group, columnIndex, repeatIndex, primitiveType));
    }

    private BigDecimal bigDecimalFromLong(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType, long value) {
        return new BigDecimal(BigInteger.valueOf(value), decimalType.getScale());
    }
}
