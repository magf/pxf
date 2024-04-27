package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.io.DataType;

import java.math.BigDecimal;
import java.math.BigInteger;

public class FixedLenByteArrayParquetTypeConverter implements ParquetTypeConverter {

    @Override
    public DataType getDataType(Type type) {
        return DataType.NUMERIC;
    }

    @Override
    public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
        int scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation()).getScale();
        return new BigDecimal(new BigInteger(group.getBinary(columnIndex, repeatIndex).getBytes()), scale);
    }

    @Override
    public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
        jsonNode.add((BigDecimal) getValue(group, columnIndex, repeatIndex, type));
    }

    @Override
    public String getValueFromList(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType) {
        return String.valueOf(getValue(group, columnIndex, repeatIndex, primitiveType));
    }
}
