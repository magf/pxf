package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;

import java.nio.ByteBuffer;

public class BinaryParquetTypeConverter implements ParquetTypeConverter {

    private final PgUtilities pgUtilities = new PgUtilities();

    @Override
    public DataType getDataType(Type type) {
        LogicalTypeAnnotation originalType = type.getLogicalTypeAnnotation();
        if (originalType == null) {
            return DataType.BYTEA;
        } else if (originalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return DataType.DATE;
        } else if (originalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            return DataType.TIMESTAMP;
        } else {
            return DataType.TEXT;
        }
    }

    @Override
    public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
        if (getDataType(type) == DataType.BYTEA) {
            jsonNode.add(group.getBinary(columnIndex, repeatIndex).getBytes());
        } else {
            jsonNode.add(group.getString(columnIndex, repeatIndex));
        }
    }

    @Override
    public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
        if (getDataType(type) == DataType.BYTEA) {
            return group.getBinary(columnIndex, repeatIndex).getBytes();
        } else {
            return group.getString(columnIndex, repeatIndex);
        }
    }

    @Override
    public String getValueFromList(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType) {
        Object value = getValue(group, columnIndex, repeatIndex, primitiveType);
        if (primitiveType.getLogicalTypeAnnotation() == null) {
            ByteBuffer byteBuffer = ByteBuffer.wrap((byte[]) value);
            return pgUtilities.encodeByteaHex(byteBuffer);
        } else {
            return String.valueOf(value);
        }
    }
}
