package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.parquet.example.data.Group;
import org.greenplum.pxf.api.io.DataType;

import java.nio.charset.StandardCharsets;

public interface ParquetTypeConverter {
    String HEX_PREPEND = "\\x";
    String HEX_PREPEND_2 = "\\\\x";
    String FILTER_COLUMN = "~~filter~~";

    DataType getDataType();

    Object read(Group group, int columnIndex, int repeatIndex);

    void write(Group group, int columnIndex, Object fieldValue);

    default Object filterValue(String val) {
        return val;
    }
    
    default byte[] readByteArray(String val) throws DecoderException {
        int beginIndex = val.startsWith(HEX_PREPEND) ? HEX_PREPEND.length() : (val.startsWith(HEX_PREPEND_2) ? HEX_PREPEND.length() : -1);
        if (beginIndex > 0) {
            return Hex.decodeHex(val.substring(beginIndex));
        } else {
            return val.getBytes(StandardCharsets.UTF_8);
        }
    }

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
