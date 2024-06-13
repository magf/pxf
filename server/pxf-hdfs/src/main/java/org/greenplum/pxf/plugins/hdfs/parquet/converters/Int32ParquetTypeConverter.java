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
import java.util.Objects;

import static org.greenplum.pxf.api.GreenplumDateTime.NANOS_IN_MICROS;
import static org.greenplum.pxf.api.GreenplumDateTime.NANOS_IN_MILLIS;

public class Int32ParquetTypeConverter implements ParquetTypeConverter {

    private final Type type;
    private final DataType dataType;
    private final DataType detectedDataType;

    public Int32ParquetTypeConverter(Type type, DataType dataType) {
        this.type = type;
        this.dataType = dataType;
        this.detectedDataType = detectDataType();
    }

    @Override
    public DataType getDataType() {
        return detectedDataType;
    }

    private DataType detectDataType() {
        LogicalTypeAnnotation originalType = type.getLogicalTypeAnnotation();
        if (originalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return DataType.DATE;
        } else if (originalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            return DataType.NUMERIC;
        } else if (originalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
            return DataType.TIME;
        } else if (originalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
            LogicalTypeAnnotation.IntLogicalTypeAnnotation intType = (LogicalTypeAnnotation.IntLogicalTypeAnnotation) originalType;
            if (intType.getBitWidth() == 8 || intType.getBitWidth() == 16) {
                return DataType.SMALLINT;
            }
        }
        return DataType.INTEGER;
    }

    @Override
    public void write(Group group, int columnIndex, Object fieldValue) {
        group.add(columnIndex, writeValue(fieldValue));
    }

    @Override
    public Integer filterValue(String val) {
        if (detectedDataType == DataType.SMALLINT || detectedDataType == DataType.INTEGER) {
            return Integer.parseInt(val);
        }
        return writeValue(val);
    }

    private int writeValue(Object fieldValue) {
        if (detectedDataType == DataType.DATE) {
            String dateString = (String) fieldValue;
            return ParquetTimestampUtilities.getDaysFromEpochFromDateString(dateString);
        } else if (detectedDataType == DataType.SMALLINT) {
            return ((Number) fieldValue).shortValue();
        } else if (detectedDataType == DataType.NUMERIC) {
            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation anno32 = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
            return strToDecimal32((String) fieldValue, anno32);
        } else if (detectedDataType == DataType.TIME) {
            String timeValue = (String) fieldValue;
            return writeTimeValue(timeValue);
        } else {
            return (Integer) fieldValue;
        }
    }

    /**
     * Converts a "time" string to a INT32.
     *
     * Times with time zone are not supported
     * https://wiki.postgresql.org/wiki/Don't_Do_This#Don.27t_use_timetz
     *
     * @param timeValue the greenplum string of the timestamp with the time zone
     * @return # of time units provided by logical type annotation since Unix epoch
     */
    private int writeTimeValue(String timeValue) {
        LocalTime time = LocalTime.parse(timeValue, GreenplumDateTime.TIME_FORMATTER);
        LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeAnno = (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
        if (Objects.requireNonNull(timeAnno.getUnit()) == LogicalTypeAnnotation.TimeUnit.MILLIS) {
            return (int) time.getLong(ChronoField.MILLI_OF_DAY);
        }
        throw new IllegalArgumentException("Unsupported time unit " + timeAnno.getUnit());
    }

    @Override
    @SuppressWarnings("deprecation")
    public Object read(Group group, int columnIndex, int repeatIndex) {
        int value = group.getInteger(columnIndex, repeatIndex);
        if (detectedDataType == DataType.DATE) {
            return new org.apache.hadoop.hive.serde2.io.DateWritable(value).get(true);
        } else if (detectedDataType == DataType.NUMERIC) {
            return bigDecimalFromLong((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation(), value);
        } else if (detectedDataType == DataType.TIME) {
            return readTime(value);
        } else if (detectedDataType == DataType.SMALLINT) {
            return (short) value;
        }
        return value;
    }

    private String readTime(int value) {
        LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeAnno = (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
        LocalTime time;
        if (Objects.requireNonNull(timeAnno.getUnit()) == LogicalTypeAnnotation.TimeUnit.MILLIS) {
            time = LocalTime.ofNanoOfDay((long) value * NANOS_IN_MILLIS);
        } else {
            throw new IllegalArgumentException("Unsupported time unit " + timeAnno.getUnit());
        }
        return time.format(GreenplumDateTime.TIME_FORMATTER);
    }

    @Override
    public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, ArrayNode jsonNode) {
        jsonNode.add(group.getInteger(columnIndex, repeatIndex));
    }

    private BigDecimal bigDecimalFromLong(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType, long value) {
        return new BigDecimal(BigInteger.valueOf(value), decimalType.getScale(), MathContext.DECIMAL32);
    }

    private static int strToDecimal32(String fieldValue, LogicalTypeAnnotation.DecimalLogicalTypeAnnotation anno32) {
        return new BigDecimal(fieldValue, MathContext.DECIMAL32).setScale(anno32.getScale(), RoundingMode.HALF_EVEN).unscaledValue().intValue();
    }
}
