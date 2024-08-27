package org.greenplum.pxf.plugins.hdfs.parquet.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetTypeConverterFactory;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetUtilities;
import org.greenplum.pxf.plugins.hdfs.utilities.PgArrayBuilder;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;

import java.util.List;

public class ListParquetTypeConverter implements ParquetTypeConverter {
    private final Type type;
    private final Type elementType;
    private final ParquetTypeConverter elementConverter;
    private final PgUtilities pgUtilities;
    private final ParquetUtilities parquetUtilities;

    public ListParquetTypeConverter(Type type, DataType dataType, ParquetTypeConverterFactory parquetTypeConverterFactory) {
        this.type = type;
        this.elementType = getElementType(type.asGroupType());
        validateElementTypeInListType(elementType);
        this.elementConverter = parquetTypeConverterFactory.create(elementType, dataType.isArrayType() ? dataType.getTypeElem() : dataType);
        this.pgUtilities = new PgUtilities();
        this.parquetUtilities = new ParquetUtilities(pgUtilities);
    }

    @Override
    public DataType getDataType() {
        DataType converterDataType = elementConverter.getDataType();
        // multidimensional arrays in gpdb don't have separate oids, they have the same oid as corresponding array type
        // independent of number of dimensions
        if (converterDataType.isArrayType()) {
            return converterDataType;
        }
        return converterDataType.getTypeArray();
    }

    @Override
    public Object read(Group group, int columnIndex, int repeatIndex) {
        PgArrayBuilder pgArrayBuilder = new PgArrayBuilder(pgUtilities);
        pgArrayBuilder.startArray();

        Group listGroup = group.getGroup(columnIndex, repeatIndex);
        // a listGroup can have any number of repeatedGroups
        int repetitionCount = listGroup.getFieldRepetitionCount(0);
        boolean elementNeedsEscapingInArray = elementConverter.getDataType().getNeedsEscapingInArray();

        for (int i = 0; i < repetitionCount; i++) {
            Group repeatedGroup = listGroup.getGroup(0, i);
            // each repeatedGroup can only have no more than 1 element
            // 0 means it is a null primitive element
            if (repeatedGroup.getFieldRepetitionCount(0) == 0) {
                pgArrayBuilder.addElement((String) null);
            } else {
                // add the non-null element into array
                String elementValue = elementConverter.readFromList(repeatedGroup, 0, 0);
                pgArrayBuilder.addElement(elementValue, elementType.isPrimitive() && elementNeedsEscapingInArray);
            }
        }
        pgArrayBuilder.endArray();
        return pgArrayBuilder.toString();
    }

    @Override
    public void write(Group group, int columnIndex, Object fieldValue) {
        LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
        if (logicalTypeAnnotation == null) {
            throw new UnsupportedTypeException("Parquet group type without logical annotation is not supported");
        }

        if (logicalTypeAnnotation != LogicalTypeAnnotation.listType()) {
            throw new UnsupportedTypeException(String.format("Parquet complex type %s is not supported", logicalTypeAnnotation));
        }
        /*
         * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
         * Parquet LIST must always annotate a 3-level structure:
         * <list-repetition> group <name> (LIST) {            // listType, a listType always has only 1 repeatedType
         *   repeated group list {                            // repeatedType, a repeatedType always has only 1 element type
         *     <element-repetition> <element-type> element;   // elementType
         *   }
         * }
         */
        GroupType listType = type.asGroupType();
        GroupType repeatedType = listType.getType(0).asGroupType();
        PrimitiveType elementType = repeatedType.getType(0).asPrimitiveType();
        // Decode Postgres String representation of an array into a list of Objects
        List<Object> values = parquetUtilities.parsePostgresArray(fieldValue.toString(), elementType.getPrimitiveTypeName(), elementType.getLogicalTypeAnnotation());

        /*
         * For example, the value of a text array ["hello","",null,"test"] would look like:
         * text_arr
         *    list
         *      element: hello
         *    list
         *      element:         --> empty element ""
         *    list               --> NULL element
         *    list
         *      element: test
         */
        Group listGroup = group.addGroup(columnIndex);
        for (Object value : values) {
            Group repeatedGroup = listGroup.addGroup(0);
            if (value != null) {
                elementConverter.write(repeatedGroup, 0, value);
            }
        }
    }

    @Override
    public Object filterValue(String val) {
        throw new UnsupportedOperationException("Filtering on list values is unsupported for column " + FILTER_COLUMN);
    }

    @Override
    public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, ArrayNode jsonNode) {
        String complexTypeName = getComplexTypeName(type.asGroupType());
        throw new UnsupportedTypeException(String.format("Parquet LIST of %s is not supported.", complexTypeName));
    }

    @Override
    public String readFromList(Group group, int columnIndex, int repeatIndex) {
        return String.valueOf(read(group, columnIndex, repeatIndex));
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
    @SuppressWarnings("deprecation")
    private void validateElementTypeInListType(Type elementType) {
        if (!elementType.isPrimitive() && !org.apache.parquet.schema.OriginalType.LIST.equals(elementType.asGroupType().getOriginalType())) {
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
