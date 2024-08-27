package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.greenplum.pxf.api.io.DataType;

public class DoubleParquetTypeConverter implements ParquetTypeConverter {

    @Override
    public DataType getDataType() {
        return DataType.FLOAT8;
    }

    @Override
    public Object read(Group group, int columnIndex, int repeatIndex) {
        return group.getDouble(columnIndex, repeatIndex);
    }

    @Override
    public void write(Group group, int columnIndex, Object fieldValue) {
        group.add(columnIndex, (Double) fieldValue);
    }

    @Override
    public Double filterValue(String val) {
        return val != null ? Double.parseDouble(val) : null;
    }

    @Override
    public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, ArrayNode jsonNode) {
        jsonNode.add(group.getDouble(columnIndex, repeatIndex));
    }
}
