package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetIntervalUtilities;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetUUIDUtilities;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;

import java.nio.ByteBuffer;

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
            return dataType == DataType.INTERVAL || dataType == DataType.UUID || dataType == DataType.JSONB ? dataType : DataType.BYTEA;
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
        } else if (detectedDataType == DataType.JSONB) {
            return readBSON(group.getBinary(columnIndex, repeatIndex).getBytes());
        } else if (detectedDataType == DataType.INTERVAL) {
            // we don't write intervals as binary, so only reading is supported for compatibility with external sources
            return ParquetIntervalUtilities.read(group.getBinary(columnIndex, repeatIndex).getBytes());
        } else if (detectedDataType == DataType.UUID) {
            // we don't write uuids as binary, so only reading is supported for compatibility with external sources
            return ParquetUUIDUtilities.readUUID(group.getBinary(columnIndex, repeatIndex).getBytes());
        } else {
            return group.getString(columnIndex, repeatIndex);
        }
    }

    @Override
    public void write(Group group, int columnIndex, Object fieldValue) {
        if (detectedDataType == DataType.TEXT || detectedDataType == DataType.JSON
                || detectedDataType == DataType.NUMERIC || dataType == DataType.BPCHAR) {
            String strVal = (String) fieldValue;
            /*
             * We need to right trim the incoming value from Greenplum. This is
             * consistent with the behaviour in Hive, where char fields are right
             * trimmed during write. Note that String and varchar Hive types are
             * not right trimmed. Hive does not trim tabs or newlines
             */
            group.add(columnIndex, dataType == DataType.BPCHAR ? Utilities.rightTrimWhiteSpace(strVal) : strVal);
        } else if (detectedDataType == DataType.JSONB) {
            String strVal = (String) fieldValue;
            group.add(columnIndex, writeBSON(strVal));
        } else if (fieldValue instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) fieldValue;
            group.add(columnIndex, Binary.fromReusedByteArray(byteBuffer.array(), 0, byteBuffer.limit()));
        } else {
            group.add(columnIndex, Binary.fromReusedByteArray((byte[]) fieldValue));
        }
    }

    private static Binary writeBSON(String strVal) {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        EncoderContext encoderContext = EncoderContext.builder().isEncodingCollectibleDocument(true).build();
        BsonDocument bsonDocument = BsonDocument.parse(strVal);
        new BsonDocumentCodec().encode(writer, bsonDocument, encoderContext);
        return Binary.fromReusedByteArray(outputBuffer.toByteArray());
    }

    private static String readBSON(byte[] bson) {
        BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bson));
        BsonDocument bsonDocument = new BsonDocumentCodec().decode(reader, DecoderContext.builder().checkedDiscriminator(false).build());
        return bsonDocument.toJson();
    }

    @Override
    public Binary filterValue(String val) {
        if (detectedDataType == DataType.BYTEA) {
            try {
                return Binary.fromReusedByteArray(readByteArray(val));
            } catch (DecoderException e) {
                throw new IllegalArgumentException(e);
            }
        } else if (detectedDataType == DataType.JSONB) {
            return writeBSON(val);
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
