package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetTimestampUtilities;

import static org.greenplum.pxf.plugins.hdfs.ParquetResolver.TIMESTAMP_PATTERN;

public class Int96ParquetTypeConverter implements ParquetTypeConverter {

    private final boolean useLocalPxfTimezoneRead;
    private final boolean useLocalPxfTimezoneWrite;
    private final DataType dataType;

    public Int96ParquetTypeConverter(DataType dataType, boolean useLocalPxfTimezoneRead, boolean useLocalPxfTimezoneWrite) {
        this.dataType = dataType;
        this.useLocalPxfTimezoneRead = useLocalPxfTimezoneRead;
        this.useLocalPxfTimezoneWrite = useLocalPxfTimezoneWrite;
    }

    @Override
    public DataType getDataType() {
        return dataType == DataType.TIMESTAMP_WITH_TIME_ZONE ? DataType.TIMESTAMP_WITH_TIME_ZONE : DataType.TIMESTAMP;
    }

    @Override
    public String read(Group group, int columnIndex, int repeatIndex) {
        return ParquetTimestampUtilities.bytesToTimestamp(
                group.getInt96(columnIndex, repeatIndex).getBytes(),
                useLocalPxfTimezoneRead,
                dataType == DataType.TIMESTAMP_WITH_TIME_ZONE);
    }

    @Override
    public void write(Group group, int columnIndex, Object fieldValue) {
        // SQL standard timestamp string value with or without time zone literals: https://www.postgresql.org/docs/9.4/datatype-datetime.html
        String timestamp = (String) fieldValue;
        if (TIMESTAMP_PATTERN.matcher(timestamp).find()) {
            // Note: this conversion convert type "timestamp with time zone" will lose timezone information
            // while preserving the correct value. (as Parquet doesn't support timestamp with time zone)
            group.add(columnIndex, ParquetTimestampUtilities.getBinaryFromTimestampWithTimeZone(timestamp));
        } else {
            group.add(columnIndex, ParquetTimestampUtilities.getBinaryFromTimestamp(timestamp, useLocalPxfTimezoneWrite));
        }
    }

    @Override
    public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, ArrayNode jsonNode) {
        String timestamp = read(group, columnIndex, repeatIndex);
        jsonNode.add(timestamp);
    }

}
