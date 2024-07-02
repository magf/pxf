package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.plugins.hdfs.parquet.converters.*;

import java.util.Optional;

public class ParquetTypeConverterFactory {
    private final ParquetConfig parquetConfig;

    public ParquetTypeConverterFactory(ParquetConfig parquetConfig) {
        this.parquetConfig = parquetConfig;
    }

    @SuppressWarnings("deprecation")
    public ParquetTypeConverter create(Type type, DataType dataType) {
        if (type.isPrimitive()) {
            PrimitiveType.PrimitiveTypeName primitiveTypeName = type.asPrimitiveType().getPrimitiveTypeName();
            if (primitiveTypeName == null) {
                throw new PxfRuntimeException("Invalid Parquet primitive schema. Parquet primitive type name is null.");
            }
            try {
                return Optional.ofNullable(createParquetTypeConverter(type, dataType, primitiveTypeName))
                        .orElseThrow(() -> new UnsupportedTypeException(
                                String.format("Parquet type converter %s is not supported", primitiveTypeName.name()))
                        );
            } catch (IllegalArgumentException e) {
                throw new UnsupportedTypeException(String.format("Primitive parquet type %s is not supported, error: %s", primitiveTypeName, e));
            }
        }

        // parquet LIST type
        GroupType groupType = type.asGroupType();
        if (groupType.getOriginalType() == org.apache.parquet.schema.OriginalType.LIST) {
            return new ListParquetTypeConverter(type, dataType, this);
        } else {
            throw new UnsupportedTypeException(String.format("Parquet complex type converter [%s] is not supported",
                    groupType.getOriginalType() != null ? groupType.getOriginalType() : "customized struct"));
        }
    }

    private ParquetTypeConverter createParquetTypeConverter(Type type, DataType dataType, PrimitiveType.PrimitiveTypeName primitiveTypeName) {
        switch (primitiveTypeName) {
            case INT32:
                return new Int32ParquetTypeConverter(type, dataType);
            case INT64:
                return new Int64ParquetTypeConverter(type, dataType, parquetConfig.isUseLocalPxfTimezoneRead(), parquetConfig.isUseLocalPxfTimezoneWrite());
            case INT96:
                return new Int96ParquetTypeConverter(parquetConfig.isUseLocalPxfTimezoneRead(), parquetConfig.isUseLocalPxfTimezoneWrite());
            case BINARY:
                return new BinaryParquetTypeConverter(type, dataType);
            case BOOLEAN:
                return new BooleanParquetTypeConverter();
            case DOUBLE:
                return new DoubleParquetTypeConverter();
            case FIXED_LEN_BYTE_ARRAY:
                return new FixedLenByteArrayParquetTypeConverter(type, dataType, parquetConfig.getDecimalUtilities());
            case FLOAT:
                return new FloatParquetTypeConverter();
            default:
                return null;
        }
    }
}
