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
import org.greenplum.pxf.plugins.hdfs.utilities.DecimalUtilities;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

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
        } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.IntervalLogicalTypeAnnotation || dataType == DataType.INTERVAL) {
            return DataType.INTERVAL;
        } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
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
            return readUUID(group.getBinary(columnIndex, repeatIndex).getBytes());
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
            return writeUUID((String) fieldValue);
        } else {
            return Binary.fromReusedByteArray((byte[]) fieldValue);
        }
    }

    private String readUUID(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long most = bb.getLong();
        long least = bb.getLong();
        return new UUID(most, least).toString();
    }

    private Binary writeUUID(String str) {
        UUID uuid = UUID.fromString(str);
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return Binary.fromReusedByteArray(bb.array());
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
