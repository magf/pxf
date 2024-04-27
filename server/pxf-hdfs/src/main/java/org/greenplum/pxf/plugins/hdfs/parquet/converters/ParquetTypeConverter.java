package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.io.DataType;

public interface ParquetTypeConverter {

    DataType getDataType(Type type);

    Object getValue(Group group, int columnIndex, int repeatIndex, Type type);

    void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode);

    /**
     * Get the String value of each primitive element from the list
     *
     * @param group         contains parquet schema and data for a row
     * @param columnIndex   is the index of the column in the row that needs to be resolved
     * @param repeatIndex   is the index of each repeated group in the list group at the column
     * @param primitiveType is the primitive type of the primitive element we are going to get
     * @return the String value of the primitive element
     */
    String getValueFromList(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType);
}
