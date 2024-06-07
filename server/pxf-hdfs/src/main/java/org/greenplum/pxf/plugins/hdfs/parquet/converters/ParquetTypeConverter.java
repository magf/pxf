package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.io.DataType;

public interface ParquetTypeConverter {

    DataType getDataType();

    Object read(Group group, int columnIndex, int repeatIndex);

    void write(Group group, int columnIndex, Object fieldValue);

    void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, ArrayNode jsonNode);

    /**
     * Get the String value of each primitive element from the list
     *
     * @param group         contains parquet schema and data for a row
     * @param columnIndex   is the index of the column in the row that needs to be resolved
     * @param repeatIndex   is the index of each repeated group in the list group at the column
     * @return the String value of the primitive element
     */
    default String readFromList(Group group, int columnIndex, int repeatIndex) {
        return String.valueOf(read(group, columnIndex, repeatIndex));
    }
}
