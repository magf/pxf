package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.BinaryValue;
import org.apache.parquet.example.data.simple.Primitive;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetIntervalUtilities;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.postgresql.util.PGInterval;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.UUID;

@Slf4j
public class BinaryParquetTypeConverter implements ParquetTypeConverter {

    private final PgUtilities pgUtilities = new PgUtilities();
    private final Type type;
    private final DataType dataType;
    private final DataType detectedDataType;

    public BinaryParquetTypeConverter(Type type, DataType dataType) {
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
        if (originalType == null) {
            return DataType.BYTEA;
        } else if (originalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return DataType.DATE;
        } else if (originalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            return DataType.NUMERIC;
        } else if (originalType instanceof LogicalTypeAnnotation.BsonLogicalTypeAnnotation) {
            return DataType.JSONB;
        } else if (originalType instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation) {
            return DataType.JSON;
        } else if (dataType == DataType.BPCHAR) {
            return DataType.BPCHAR;
        } else {
            return DataType.TEXT;
        }
    }

    @Override
    public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, ArrayNode jsonNode) {
        Object val = read(group, columnIndex, repeatIndex);
        if (getDataType() == DataType.BYTEA) {
            jsonNode.add((byte[]) val);
        } else {
            jsonNode.add((String) val);
        }
    }

    @Override
    public Object read(Group group, int columnIndex, int repeatIndex) {
        if (detectedDataType == DataType.BYTEA) {
            return group.getBinary(columnIndex, repeatIndex).getBytes();
        } else {
            return group.getString(columnIndex, repeatIndex);
        }
    }

    @Override
    public void write(Group group, int columnIndex, Object fieldValue) {
        if (detectedDataType == DataType.TEXT || detectedDataType == DataType.JSON || detectedDataType == DataType.JSONB
                || detectedDataType == DataType.NUMERIC || dataType == DataType.BPCHAR) {
            String strVal = (String) fieldValue;
            /*
             * We need to right trim the incoming value from Greenplum. This is
             * consistent with the behaviour in Hive, where char fields are right
             * trimmed during write. Note that String and varchar Hive types are
             * not right trimmed. Hive does not trim tabs or newlines
             */
            group.add(columnIndex, dataType == DataType.BPCHAR ? Utilities.rightTrimWhiteSpace(strVal) : strVal);
        } else if (fieldValue instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) fieldValue;
            group.add(columnIndex, Binary.fromReusedByteArray(byteBuffer.array(), 0, byteBuffer.limit()));
        } else {
            group.add(columnIndex, Binary.fromReusedByteArray((byte[]) fieldValue));
        }
    }

    @Override
    public Binary filterValue(String val) {
        if (detectedDataType == DataType.BYTEA) {
            try {
                return Binary.fromReusedByteArray(readByteArray(val));
            } catch (DecoderException e) {
                throw new IllegalArgumentException(e);
            }
        }
        return Binary.fromString(val);
    }

    @Override
    public String readFromList(Group group, int columnIndex, int repeatIndex) {
        Object value = read(group, columnIndex, repeatIndex);
        if (type.getLogicalTypeAnnotation() == null) {
            ByteBuffer byteBuffer = ByteBuffer.wrap((byte[]) value);
            return pgUtilities.encodeByteaHex(byteBuffer);
        } else {
            return String.valueOf(value);
        }
    }
}
