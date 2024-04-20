package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.plugins.hdfs.parquet.converters.*;

import java.util.Optional;

public class ParquetTypeConverterFactory {
    private final ParquetConfig parquetConfig;

    public ParquetTypeConverterFactory(ParquetConfig parquetConfig) {
        this.parquetConfig = parquetConfig;
    }

    public ParquetTypeConverter create(Type type) {
        if (type.isPrimitive()) {
            PrimitiveType.PrimitiveTypeName primitiveTypeName = type.asPrimitiveType().getPrimitiveTypeName();
            if (primitiveTypeName == null) {
                throw new PxfRuntimeException("Invalid Parquet primitive schema. Parquet primitive type name is null.");
            }
            try {
                PxfParquetType pxfParquetType = PxfParquetType.valueOf(primitiveTypeName.name());
                return Optional.ofNullable(createParquetTypeConverter(pxfParquetType))
                        .orElseThrow(() -> new UnsupportedTypeException(
                                String.format("Primitive parquet type converter %s is not supported", pxfParquetType))
                        );
            } catch (IllegalArgumentException e) {
                throw new UnsupportedTypeException(String.format("Primitive parquet type %s is not supported, error: %s", primitiveTypeName, e));
            }
        }

        String complexTypeName = getComplexTypeName(type.asGroupType());
        try {
            // parquet LIST type
            PxfParquetType pxfParquetType = PxfParquetType.valueOf(complexTypeName);
            return Optional.ofNullable(createParquetTypeConverter(pxfParquetType))
                    .orElseThrow(() -> new UnsupportedTypeException(String.format("Complex parquet type converter %s is not supported", pxfParquetType)));
        } catch (IllegalArgumentException e) {
            // other unsupported parquet complex type
            throw new UnsupportedTypeException(String.format("Parquet complex type %s is not supported, error: %s", complexTypeName, e));
        }
    }

    /**
     * Get the type name of the input complex type
     *
     * @param complexType the GroupType we want to get the type name from
     * @return the type name of the complex type
     */
    private String getComplexTypeName(GroupType complexType) {
        return complexType.getOriginalType() == null ? "customized struct" : complexType.getOriginalType().name();
    }

    private ParquetTypeConverter createParquetTypeConverter(PxfParquetType pxfParquetType) {
        switch (pxfParquetType) {
            case INT32:
                return new Int32ParquetTypeConverter();
            case INT64:
                return new Int64ParquetTypeConverter(parquetConfig.isUseLocalPxfTimezoneRead());
            case INT96:
                return new Int96ParquetTypeConverter(parquetConfig.isUseLocalPxfTimezoneRead());
            case BINARY:
                return new BinaryParquetTypeConverter();
            case BOOLEAN:
                return new BooleanParquetTypeConverter();
            case DOUBLE:
                return new DoubleParquetTypeConverter();
            case FIXED_LEN_BYTE_ARRAY:
                return new FixedLenByteArrayParquetTypeConverter();
            case FLOAT:
                return new FloatParquetTypeConverter();
            case LIST:
                return new ListParquetTypeConverter(this);
            default:
                return null;
        }
    }
}
