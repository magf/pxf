package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.GreenplumDateTime;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetTimestampUtilities;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.LocalTime;
import java.time.temporal.ChronoField;

import static org.greenplum.pxf.api.GreenplumDateTime.NANOS_IN_MICROS;
import static org.greenplum.pxf.api.GreenplumDateTime.NANOS_IN_MILLIS;
import static org.greenplum.pxf.plugins.hdfs.ParquetResolver.TIMESTAMP_PATTERN;

public class Int64ParquetTypeConverter implements ParquetTypeConverter {
    private final boolean useLocalPxfTimezoneRead;
    private final boolean useLocalPxfTimezoneWrite;
    private final Type type;
    private final DataType dataType;
    private final DataType detectedDataType;

    public Int64ParquetTypeConverter(Type type, DataType dataType, boolean useLocalPxfTimezoneRead, boolean useLocalPxfTimezoneWrite) {
        this.type = type;
        this.dataType = dataType;
        this.useLocalPxfTimezoneRead = useLocalPxfTimezoneRead;
        this.useLocalPxfTimezoneWrite = useLocalPxfTimezoneWrite;
        this.detectedDataType = detectDataType();
    }

    @Override
    public DataType getDataType() {
        return detectedDataType;
    }

    public DataType detectDataType() {
        if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            return DataType.NUMERIC;
        } else if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            return dataType == DataType.TIMESTAMP_WITH_TIME_ZONE ? DataType.TIMESTAMP_WITH_TIME_ZONE : DataType.TIMESTAMP;
        } else if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
            return DataType.TIME;
        }
        return DataType.BIGINT;
    }

    @Override
    public Object read(Group group, int columnIndex, int repeatIndex) {
        long value = group.getLong(columnIndex, repeatIndex);
        if (detectedDataType == DataType.NUMERIC) {
            return bigDecimalFromLong((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation(), value);
        } else if (detectedDataType == DataType.TIMESTAMP || detectedDataType == DataType.TIMESTAMP_WITH_TIME_ZONE) {
            LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tsAnno = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
            return ParquetTimestampUtilities.getTimestampFromLong(value, tsAnno.getUnit(), useLocalPxfTimezoneRead, detectedDataType == DataType.TIMESTAMP_WITH_TIME_ZONE);
        } else if (detectedDataType == DataType.TIME) {
            return readTime(value);
        }
        return value;
    }

    @Override
    public void write(Group group, int columnIndex, Object fieldValue) {
        group.add(columnIndex, writeValue(fieldValue));
    }

    @Override
    public Long filterValue(String val) {
        if (detectedDataType == DataType.BIGINT) {
            return Long.parseLong(val);
        }
        return writeValue(val);
    }

    private long writeValue(Object fieldValue) {
        if (detectedDataType == DataType.TIMESTAMP || detectedDataType == DataType.TIMESTAMP_WITH_TIME_ZONE) {
            String timestamp = (String) fieldValue;
            boolean isTimestampWithTimeZone = TIMESTAMP_PATTERN.matcher(timestamp).find();
            return ParquetTimestampUtilities
                    .getLongFromTimestamp(timestamp, useLocalPxfTimezoneWrite, isTimestampWithTimeZone);
        } else if (detectedDataType == DataType.NUMERIC) {
            String decimalValue = (String) fieldValue;
            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalAnno = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
            return new BigDecimal(decimalValue, MathContext.DECIMAL64).setScale(decimalAnno.getScale(), RoundingMode.HALF_EVEN).unscaledValue().longValue();
        } else if (detectedDataType == DataType.TIME) {
            String timeValue = (String) fieldValue;
            return writeTimeValue(timeValue);
        } else {
            return (Long) fieldValue;
        }
    }

    private String readTime(long value) {
        LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeAnno = (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
        LocalTime time;
        switch (timeAnno.getUnit()) {
            case NANOS:
                time = LocalTime.ofNanoOfDay(value);
                break;
            case MICROS:
                time = LocalTime.ofNanoOfDay(value * NANOS_IN_MICROS);
                break;
            default:
                throw new IllegalArgumentException("Unsupported time unit " + timeAnno.getUnit());
        }
        return time.format(GreenplumDateTime.TIME_FORMATTER);
    }

    /**
     * Converts a "time" string to a INT64.
     *
     * Times with time zone are not supported
     * https://wiki.postgresql.org/wiki/Don't_Do_This#Don.27t_use_timetz
     *
     * @param timeValue the greenplum string of the timestamp with the time zone
     * @return # of time units provided by logical type annotation since Unix epoch
     */
    private long writeTimeValue(String timeValue) {
        LocalTime time = LocalTime.parse(timeValue, GreenplumDateTime.TIME_FORMATTER);
        LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeAnno = (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
        switch (timeAnno.getUnit()) {
            case NANOS:
                return time.getLong(ChronoField.NANO_OF_DAY);
            case MICROS:
                return time.getLong(ChronoField.MICRO_OF_DAY);
            default:
                throw new IllegalArgumentException("Unsupported time unit " + timeAnno.getUnit());
        }
    }

    @Override
    public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, ArrayNode jsonNode) {
        if (detectedDataType == DataType.TIMESTAMP || detectedDataType == DataType.TIMESTAMP_WITH_TIME_ZONE) {
            String timestamp = (String) read(group, columnIndex, repeatIndex);
            jsonNode.add(timestamp);
        } else {
            jsonNode.add(group.getLong(columnIndex, repeatIndex));
        }
    }

    private BigDecimal bigDecimalFromLong(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType, long value) {
        return new BigDecimal(BigInteger.valueOf(value), decimalType.getScale());
    }
}
