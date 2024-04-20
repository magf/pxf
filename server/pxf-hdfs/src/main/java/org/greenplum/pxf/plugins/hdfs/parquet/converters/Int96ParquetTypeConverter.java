package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetTimestampUtilities;

public class Int96ParquetTypeConverter implements ParquetTypeConverter {

    @Override
    public DataType getDataType(Type type) {
        return DataType.TIMESTAMP;
    }

    @Override
    public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
        return ParquetTimestampUtilities.bytesToTimestamp(group.getInt96(columnIndex, repeatIndex).getBytes());
    }

    @Override
    public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
        String timestamp = (String) getValue(group, columnIndex, repeatIndex, type);
        jsonNode.add(timestamp);
    }

    @Override
    public String getValueFromList(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType) {
        return String.valueOf(getValue(group, columnIndex, repeatIndex, primitiveType));
    }
}
