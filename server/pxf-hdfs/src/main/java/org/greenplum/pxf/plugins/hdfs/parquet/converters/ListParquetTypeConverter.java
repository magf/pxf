package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetTypeConverterFactory;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetUtilities;
import org.greenplum.pxf.plugins.hdfs.utilities.PgArrayBuilder;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;

public class ListParquetTypeConverter implements ParquetTypeConverter {
    private final ParquetTypeConverterFactory parquetTypeConverterFactory;
    private final PgUtilities pgUtilities;

    public ListParquetTypeConverter(ParquetTypeConverterFactory parquetTypeConverterFactory) {
        this.parquetTypeConverterFactory = parquetTypeConverterFactory;
        this.pgUtilities = new PgUtilities();
    }

    @Override
    public DataType getDataType(Type type) {
        Type elementType = getElementType(type.asGroupType());
        validateElementTypeInListType(elementType);
        return parquetTypeConverterFactory.create(elementType).getDataType(elementType).getTypeArray();
    }

    @Override
    public Object getValue(Group group, int columnIndex, int repeatIndex, Type type) {
        PgArrayBuilder pgArrayBuilder = new PgArrayBuilder(pgUtilities);
        pgArrayBuilder.startArray();

        Group listGroup = group.getGroup(columnIndex, repeatIndex);
        // a listGroup can have any number of repeatedGroups
        int repetitionCount = listGroup.getFieldRepetitionCount(0);
        Type elementType = getElementType(type.asGroupType());
        validateElementTypeInListType(elementType);
        ParquetTypeConverter elementConverter = parquetTypeConverterFactory.create(elementType);
        boolean elementNeedsEscapingInArray = elementConverter.getDataType(elementType).getNeedsEscapingInArray();

        for (int i = 0; i < repetitionCount; i++) {
            Group repeatedGroup = listGroup.getGroup(0, i);
            // each repeatedGroup can only have no more than 1 element
            // 0 means it is a null primitive element
            if (repeatedGroup.getFieldRepetitionCount(0) == 0) {
                pgArrayBuilder.addElement((String) null);
            } else {
                // add the non-null element into array
                String elementValue = elementConverter.getValueFromList(repeatedGroup, 0, 0, elementType.asPrimitiveType());
                pgArrayBuilder.addElement(elementValue, elementNeedsEscapingInArray);
            }
        }
        pgArrayBuilder.endArray();
        return pgArrayBuilder.toString();
    }

    @Override
    public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, Type type, ArrayNode jsonNode) {
        String complexTypeName = type.asGroupType().getOriginalType() == null ?
                "customized struct" :
                type.asGroupType().getOriginalType().name();
        throw new UnsupportedTypeException(String.format("Parquet LIST of %s is not supported.", complexTypeName));
    }

    @Override
    public String getValueFromList(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType) {
        return String.valueOf(getValue(group, columnIndex, repeatIndex, primitiveType));
    }

    /*
     * Get the Parquet primitive schema type based on Parquet List schema type
     *
     * Parquet List Schema
     * <list-repetition> group <name> (LIST) {
     *   repeated group list {
     *     <element-repetition> <element-type> element;
     *   }
     * }
     *
     * - The outer-most level must be a group annotated with `LIST` that contains a single field named `list`. The repetition of this level must be either `optional` or `required` and determines whether the list is nullable.
     * - The middle level, named `list`, must be a repeated group with a single field named `element`.
     * - The `element` field encodes the list's element type and repetition. Element repetition must be `required` or `optional`.
     */
    private Type getElementType(GroupType listType) {
        ParquetUtilities.validateListSchema(listType);

        GroupType repeatedType = listType.getType(0).asGroupType();
        return repeatedType.getType(0);
    }

    /**
     * Validate whether the element type in Parquet List type is supported by pxf
     *
     * @param elementType the element type of Parquet List type
     */
    private void validateElementTypeInListType(Type elementType) {
        if (!elementType.isPrimitive()) {
            String complexTypeName = getComplexTypeName(elementType.asGroupType());
            throw new UnsupportedTypeException(String.format("Parquet LIST of %s is not supported.", complexTypeName));
        }
    }

    /**
     * Get the type name of the input complex type
     *
     * @param complexType the GroupType we want to get the type name from
     * @return the type name of the complex type
     */
    private static String getComplexTypeName(GroupType complexType) {
        return complexType.getOriginalType() == null ? "customized struct" : complexType.getOriginalType().name();
    }
}
