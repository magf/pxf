package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.commons.lang.StringUtils;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;


/**
 * Utility methods for converting Postgres types text format into Java objects according to primitiveTypeName
 */
public class ParquetUtilities {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetUtilities.class);

    private final PgUtilities pgUtilities;

    public ParquetUtilities(PgUtilities pgUtilities) {
        this.pgUtilities = pgUtilities;
    }

    /**
     * Check whether the provided Parquet lIST schema is a valid one
     *
     * @param listType Parquet LIST schema
     */
    public static void validateListSchema(GroupType listType) {
        if (listType.getFields().size() != 1 || listType.getType(0).isPrimitive() || listType.getType(0).asGroupType().getFields().size() != 1) {
            throw new PxfRuntimeException(String.format("Invalid Parquet List schema: %s.", listType.toString().replace("\n", " ")));
        }
    }

    /**
     * Parse a String representation of Greengage array into a Parquet object array according to primitiveTypeName
     *
     * @param val                   A String representation of a Greengage array
     * @param primitiveTypeName     Primitive type name of each element inside the array
     * @param logicalTypeAnnotation Logical type annotation if this type has one
     * @return A list of Java object
     */
    public List<Object> parsePostgresArray(String val, PrimitiveType.PrimitiveTypeName primitiveTypeName, LogicalTypeAnnotation logicalTypeAnnotation) {
        LOG.trace("schema type={}, value={}", primitiveTypeName, val);

        if (val == null) {
            return null;
        }
        String[] splits = pgUtilities.splitArray(val);
        List<Object> data = new ArrayList<>(splits.length);
        for (String split : splits) {
            try {
                data.add(decodeString(split, primitiveTypeName, logicalTypeAnnotation));
            } catch (NumberFormatException | PxfRuntimeException | DateTimeParseException e) {
                String hint = createErrorHintFromValue(StringUtils.startsWith(split, "{"), val);
                throw new PxfRuntimeException(String.format("Error parsing array element: %s was not of expected type %s", split, primitiveTypeName), hint, e);
            }
        }
        return data;
    }

    /**
     * Decode a String representation of a Greengage object into a Java object according to primitiveTypeName
     *
     * @param val                   A String representation of a Greengage object
     * @param primitiveTypeName     Primitive type name of this object
     * @param logicalTypeAnnotation Logical type annotation of this object if it has one
     * @return A Java object
     */
    private Object decodeString(String val, PrimitiveType.PrimitiveTypeName primitiveTypeName, LogicalTypeAnnotation logicalTypeAnnotation) {
        if (val == null) {
            return null;
        }
        switch (primitiveTypeName) {
            case BINARY:
                if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
                    || logicalTypeAnnotation instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation
                    || logicalTypeAnnotation instanceof LogicalTypeAnnotation.BsonLogicalTypeAnnotation) {
                    return val;
                } else {
                    return pgUtilities.parseByteaLiteral(val);
                }
            case BOOLEAN:
                //parquet bool val is "true" or "false" but pgUtilities only accept "t" or "f"
                return pgUtilities.parseBoolLiteral(val);
            case INT32:
                if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                    // ParquetResolver.fillGroupWithPrimitive will convert Date String into INT32 (number of days from the Unix epoch, 1 January 1970)
                    return val;
                } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation &&
                        ((LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalTypeAnnotation).getBitWidth() == 16) {
                    return Short.parseShort(val);
                } else {
                    return Integer.parseInt(val);
                }
            case INT64:
                // ":" here is only for purpose of supporting libraries that don't support new parquet annotations (spark for example)
                if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
                    || logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation || val.contains(":")) {
                    return val;
                } else {
                    return Long.parseLong(val);
                }
            case DOUBLE:
                return Double.parseDouble(val);
            case FLOAT:
                return Float.parseFloat(val);
            case INT96:
            case FIXED_LEN_BYTE_ARRAY:
                return val;
            default:
                // For now, we've covered all the Parquet Primitive types, so we won't get into this case
                throw new PxfRuntimeException(String.format("type: %s is not supported", primitiveTypeName));
        }
    }

    /**
     * Provide more detailed error message
     *
     * @param isMultiDimensional Whether this error is caused by inserting a multi-dimensional array
     * @param val                The string representation we are currently trying to decode
     * @return The generated error message
     */
    private String createErrorHintFromValue(boolean isMultiDimensional, String val) {
        if (isMultiDimensional) {
            return String.format("Column value \"%s\" is a multi-dimensional array; PXF does not support writing multi-dimensional arrays to Parquet files.", val);
        } else {
            return "Unexpected state since PXF generated the Parquet schema.";
        }
    }
}
