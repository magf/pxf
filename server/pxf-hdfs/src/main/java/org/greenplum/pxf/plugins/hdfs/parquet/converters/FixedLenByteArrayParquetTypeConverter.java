package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetFixedLenByteArrayUtilities;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetIntervalUtilities;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetUUIDUtilities;
import org.greenplum.pxf.plugins.hdfs.utilities.DecimalUtilities;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

import static org.greenplum.pxf.plugins.hdfs.parquet.ParquetIntervalUtilities.INTERVAL_TYPE_LENGTH;

@Slf4j
public class FixedLenByteArrayParquetTypeConverter implements ParquetTypeConverter {

    private final Type type;
    private final DataType dataType;
    private final DataType detectedDataType;
    private DecimalUtilities decimalUtilities;

    public FixedLenByteArrayParquetTypeConverter(Type type, DataType dataType, DecimalUtilities decimalUtilities) {
        this.type = type;
        this.dataType = dataType;
        this.decimalUtilities = decimalUtilities;
        this.detectedDataType = detectDataType();
    }

    private DataType detectDataType() {
        LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
        if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            return DataType.NUMERIC;
        } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.IntervalLogicalTypeAnnotation
                || (dataType == DataType.INTERVAL && type.asPrimitiveType().getTypeLength() == INTERVAL_TYPE_LENGTH)) {
            return DataType.INTERVAL;
        } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation
                || (dataType == DataType.UUID && type.asPrimitiveType().getTypeLength() == LogicalTypeAnnotation.UUIDLogicalTypeAnnotation.BYTES)) {
            return DataType.UUID;
        } else {
            return DataType.BYTEA;
        }
    }

    @Override
    public DataType getDataType() {
        return detectedDataType;
    }

    @Override
    public Object read(Group group, int columnIndex, int repeatIndex) {
        if (detectedDataType == DataType.NUMERIC) {
            int scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation()).getScale();
            return new BigDecimal(new BigInteger(group.getBinary(columnIndex, repeatIndex).getBytes()), scale);
        } else if (detectedDataType == DataType.INTERVAL) {
            return ParquetIntervalUtilities.read(group.getBinary(columnIndex, repeatIndex).getBytes());
        } else if (detectedDataType == DataType.UUID) {
            return ParquetUUIDUtilities.readUUID(group.getBinary(columnIndex, repeatIndex).getBytes());
        } else {
            return group.getBinary(columnIndex, repeatIndex).getBytes();
        }
    }

    @Override
    public void write(Group group, int columnIndex, Object fieldValue) {
        Binary value = writeValue(fieldValue, String.valueOf(columnIndex));
        if (value != null) {
            group.add(columnIndex, value);
        }
    }

    @Override
    public Binary filterValue(String val) {
        if (val == null) {
            return null;
        }
        if (detectedDataType == DataType.BYTEA) {
            try {
                return writeValue(readByteArray(val), "");
            } catch (DecoderException e) {
                throw new IllegalArgumentException("Couldn't decode byte array sequence for value " + val);
            }
        }
        return writeValue(val, FILTER_COLUMN);
    }

    private Binary writeValue(Object fieldValue, String columnIndex) {
        if (detectedDataType == DataType.NUMERIC) {
            byte[] fixedLenByteArray = getDecimalFixedLenByteArray((String) fieldValue, type, columnIndex);
            if (fixedLenByteArray == null) {
                return null;
            }
            return Binary.fromReusedByteArray(fixedLenByteArray);
        } else if (detectedDataType == DataType.INTERVAL) {
            return ParquetIntervalUtilities.write((String) fieldValue);
        } else if (detectedDataType == DataType.UUID) {
            return ParquetUUIDUtilities.writeUUID((String) fieldValue);
        } else {
            return Binary.fromReusedByteArray((byte[]) fieldValue);
        }
    }

    private byte[] getDecimalFixedLenByteArray(String value, Type type, String columnName) {
        return ParquetFixedLenByteArrayUtilities.convertFromBigDecimal(decimalUtilities, value, columnName,
                (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation());
    }

    @Override
    public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, ArrayNode jsonNode) {
        Object val = read(group, columnIndex, repeatIndex);
        if (detectedDataType == DataType.NUMERIC) {
            jsonNode.add((BigDecimal) val);
        } else if (detectedDataType == DataType.INTERVAL || detectedDataType == DataType.UUID) {
            jsonNode.add((String) val);
        } else {
            jsonNode.add((byte[]) val);
        }

    }
}
