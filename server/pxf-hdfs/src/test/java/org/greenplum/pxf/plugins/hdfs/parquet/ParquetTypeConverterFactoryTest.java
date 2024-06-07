package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.parquet.schema.*;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.plugins.hdfs.parquet.converters.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ParquetTypeConverterFactoryTest {
    ParquetTypeConverterFactory factory;
    ParquetConfig config;

    @BeforeEach
    void setUp() {
        config = ParquetConfig.builder().useLocalPxfTimezoneRead(true).build();
        factory = new ParquetTypeConverterFactory(config);
    }

    @Test
    void createWithNullTypeName() {
        Type type = mock(Type.class);
        PrimitiveType primitiveType = mock(PrimitiveType.class);
        when(type.isPrimitive()).thenReturn(true);
        when(type.asPrimitiveType()).thenReturn(primitiveType);
        when(primitiveType.getPrimitiveTypeName()).thenReturn(null);
        Exception e = assertThrows(PxfRuntimeException.class,
                () -> factory.create(type, DataType.UNSUPPORTED_TYPE));
        String expectedMessage = "Invalid Parquet primitive schema. Parquet primitive type name is null.";
        assertEquals(expectedMessage, e.getMessage());
    }

    @Test
    void createWithNotSupportedComplexType() {
        Type type = mock(Type.class);
        GroupType complexType = mock(GroupType.class);
        when(type.isPrimitive()).thenReturn(false);
        when(type.asGroupType()).thenReturn(complexType);
        when(complexType.getOriginalType()).thenReturn(null);
        Exception e = assertThrows(UnsupportedTypeException.class,
                () -> factory.create(type, DataType.UNSUPPORTED_TYPE));
        assertTrue(e.getMessage().contains("Parquet complex type converter [customized struct] is not supported"));
    }

    @Test
    void createBinaryConverter() {
        Type type = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.BINARY, "BINARY");
        ParquetTypeConverter converter = factory.create(type, DataType.VARCHAR);
        assertTrue(converter instanceof BinaryParquetTypeConverter);
    }

    @Test
    void createBooleanConverter() {
        Type type = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.BOOLEAN, "BOOLEAN");
        ParquetTypeConverter converter = factory.create(type, DataType.BOOLEAN);
        assertTrue(converter instanceof BooleanParquetTypeConverter);
    }

    @Test
    void createDoubleConverter() {
        Type type = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.DOUBLE, "DOUBLE");
        ParquetTypeConverter converter = factory.create(type, DataType.FLOAT8);
        assertTrue(converter instanceof DoubleParquetTypeConverter);
    }

    @Test
    void createFixedLenByteArrayConverter() {
        Type type = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, "FIXED_LEN_BYTE_ARRAY");
        ParquetTypeConverter converter = factory.create(type, DataType.NUMERIC);
        assertTrue(converter instanceof FixedLenByteArrayParquetTypeConverter);
    }

    @Test
    void createFloatConverter() {
        Type type = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.FLOAT, "FLOAT");
        ParquetTypeConverter converter = factory.create(type, DataType.REAL);
        assertTrue(converter instanceof FloatParquetTypeConverter);
    }

    @Test
    void createInt32Converter() {
        Type type = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.INT32, "INT32");
        ParquetTypeConverter converter = factory.create(type, DataType.INTEGER);
        assertTrue(converter instanceof Int32ParquetTypeConverter);
    }

    @Test
    void createInt64Converter() {
        Type type = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.INT64, "INT64");
        ParquetTypeConverter converter = factory.create(type, DataType.BIGINT);
        assertTrue(converter instanceof Int64ParquetTypeConverter);
    }

    @Test
    void createInt96Converter() {
        Type type = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.INT96, "INT96");
        ParquetTypeConverter converter = factory.create(type, DataType.TIMESTAMP);
        assertTrue(converter instanceof Int96ParquetTypeConverter);
    }

    @Test
    void createListComplexTypeConverter() {
        Type type1 = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.INT64, "INT64");
        Type complexType = Types.optionalList()
                .setElementType(type1)
                .named("LIST");
        ParquetTypeConverter converter = factory.create(complexType, DataType.TIMESTAMP);
        assertTrue(converter instanceof ListParquetTypeConverter);
    }
}
