package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.io.DataType;

public class FloatParquetTypeConverter implements ParquetTypeConverter {

    @Override
    public DataType getDataType() {
        return DataType.REAL;
    }

    @Override
    public Object read(Group group, int columnIndex, int repeatIndex) {
        return group.getFloat(columnIndex, repeatIndex);
    }

    @Override
    public void write(Group group, int columnIndex, Object fieldValue) {
        group.add(columnIndex, (Float) fieldValue);
    }

    @Override
    public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, ArrayNode jsonNode) {
        jsonNode.add(group.getFloat(columnIndex, repeatIndex));
    }
}
