package org.greenplum.pxf.plugins.hdfs;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hdfs.parquet.*;
import org.greenplum.pxf.plugins.hdfs.parquet.converters.ParquetTypeConverter;
import org.greenplum.pxf.plugins.hdfs.utilities.DecimalOverflowOption;
import org.greenplum.pxf.plugins.hdfs.utilities.DecimalUtilities;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.parquet.schema.LogicalTypeAnnotation.*;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.greenplum.pxf.plugins.hdfs.ParquetFileAccessor.DEFAULT_USE_LOCAL_PXF_TIMEZONE_WRITE;
import static org.greenplum.pxf.plugins.hdfs.ParquetFileAccessor.USE_LOCAL_PXF_TIMEZONE_WRITE_NAME;

public class ParquetResolver extends BasePlugin implements Resolver {

    // used to distinguish string pattern between type "timestamp" ("2019-03-14 14:10:28")
    // and type "timestamp with time zone" ("2019-03-14 14:10:28+07:30")
    public static final Pattern TIMESTAMP_PATTERN = Pattern.compile("[+-]\\d{2}(:\\d{2})?$");
    public static final String PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME = "pxf.parquet.write.decimal.overflow";
    public static final String USE_LOCAL_PXF_TIMEZONE_READ_NAME = "USE_LOCAL_PXF_TIMEZONE_READ";
    public static final boolean DEFAULT_USE_LOCAL_PXF_TIMEZONE_READ = true;
    private static final PgUtilities pgUtilities = new PgUtilities();
    private final ObjectMapper mapper = new ObjectMapper();
    private final ParquetUtilities parquetUtilities = new ParquetUtilities(pgUtilities);
    private MessageType schema;
    private SimpleGroupFactory groupFactory;
    private List<ColumnDescriptor> columnDescriptors;
    private DecimalUtilities decimalUtilities;
    private ParquetConfig parquetConfig;
    private ParquetTypeConverterFactory parquetTypeConverterFactory;
    private List<ParquetTypeConverter> schemaConverters;

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        columnDescriptors = context.getTupleDescription();
        DecimalOverflowOption decimalOverflowOption = DecimalOverflowOption.valueOf(configuration.get(PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, DecimalOverflowOption.ROUND.name()).toUpperCase());
        decimalUtilities = new DecimalUtilities(decimalOverflowOption, true);
        boolean useLocalPxfTimezoneWrite = context.getOption(USE_LOCAL_PXF_TIMEZONE_WRITE_NAME, DEFAULT_USE_LOCAL_PXF_TIMEZONE_WRITE);
        boolean useLocalPxfTimezoneRead = context.getOption(USE_LOCAL_PXF_TIMEZONE_READ_NAME, DEFAULT_USE_LOCAL_PXF_TIMEZONE_READ);
        parquetConfig = ParquetConfig.builder()
                .useLocalPxfTimezoneWrite(useLocalPxfTimezoneWrite)
                .useLocalPxfTimezoneRead(useLocalPxfTimezoneRead)
                .decimalUtilities(decimalUtilities)
                .build();
        parquetTypeConverterFactory = new ParquetTypeConverterFactory(parquetConfig);
    }

    /**
     * Get fields based on the row
     *
     * @param row the row to get the fields from
     * @return a list of fields containing Greenplum data type and data value
     */
    @Override
    public List<OneField> getFields(OneRow row) {
        validateSchema();
        Group group = (Group) row.getData();
        List<OneField> output = new LinkedList<>();
        int columnIndex = 0;

        for (ColumnDescriptor columnDescriptor : columnDescriptors) {
            OneField oneField;
            if (!columnDescriptor.isProjected()) {
                oneField = new OneField(columnDescriptor.columnTypeCode(), null);
            } else {
                oneField = resolveField(group, columnIndex);
                columnIndex++;
            }
            output.add(oneField);
        }
        return output;
    }

    /**
     * Constructs and sets the fields of a {@link OneRow}.
     *
     * @param record list of {@link OneField}
     * @return the constructed {@link OneRow}
     * @throws IOException if constructing a row from the fields failed
     */
    @Override
    public OneRow setFields(List<OneField> record) throws IOException {
        validateSchema();
        Group group = groupFactory.newGroup();
        for (int i = 0; i < record.size(); i++) {
            OneField field = record.get(i);
            fillGroup(group, i, field);
        }
        return new OneRow(null, group);
    }

    /**
     * Fill the element of Parquet Group at the given index with provided value
     *
     * @param group       the Parquet Group object being filled
     * @param columnIndex the index of the column in a row that needs to be filled with data
     * @param field       OneField object holding the value we need to fill into the Parquet Group object
     */
    private void fillGroup(Group group, int columnIndex, OneField field) {
        if (field.val == null) {
            return;
        }

        ParquetTypeConverter converter = schemaConverters.get(columnIndex);
        try {
            converter.write(group, columnIndex, field.val);
        } catch (UnsupportedTypeException ex) {
            throw new UnsupportedTypeException(ex.getMessage().replaceAll("(?<=\\s)" + columnIndex + "(?=\\s)", columnDescriptors.get(columnIndex).columnName()));
        }
    }

    // Set schema from context if null
    // TODO: Fix the bridge interface so the schema is set before get/setFields is called
    //       Then validateSchema can be done during initialize phase
    private void validateSchema() {
        if (schema == null) {
            schema = (MessageType) context.getMetadata();
            if (schema == null)
                throw new RuntimeException("No schema detected in request context");
            groupFactory = new SimpleGroupFactory(schema);
            schemaConverters = new ArrayList<>();
            int i = 0;
            for (ColumnDescriptor columnDescriptor : columnDescriptors) {
                if (columnDescriptor.isProjected()) {
                    schemaConverters.add(
                            parquetTypeConverterFactory.create(schema.getType(i), columnDescriptor.getDataType()));
                    i++;
                }
            }
        }
    }

    /**
     * Resolve the Parquet data at the columnIndex into Greenplum representation
     *
     * @param group       contains parquet schema and data for a row
     * @param columnIndex is the index of the column in the row that needs to be resolved
     * @return a field containing Greenplum data type and data value
     */
    private OneField resolveField(Group group, int columnIndex) {
        OneField field = new OneField();
        // get type converter based on the field data type
        // schema is the readSchema, if there is column projection, the schema will be a subset of tuple descriptions
        Type type = schema.getType(columnIndex);
        ParquetTypeConverter converter = schemaConverters.get(columnIndex);
        // determine how many values for the field are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);
        if (type.getRepetition() == REPEATED) {
            // For REPEATED type, repetitionCount can be any non-negative number,
            // the element value will be converted into JSON value
            ArrayNode jsonArray = mapper.createArrayNode();
            for (int repeatIndex = 0; repeatIndex < repetitionCount; repeatIndex++) {
                converter.addValueToJsonArray(group, columnIndex, repeatIndex, jsonArray);
            }
            field.type = DataType.TEXT.getOID();
            try {
                field.val = mapper.writeValueAsString(jsonArray);
            } catch (Exception e) {
                String typeName;
                if (type.isPrimitive()) {
                    typeName = type.asPrimitiveType().getPrimitiveTypeName().name();
                } else {
                    typeName = type.asGroupType().getLogicalTypeAnnotation() == null ?
                            "customized struct" : type.asGroupType().getLogicalTypeAnnotation().toString();
                }
                throw new RuntimeException(String.format("Failed to serialize repeated parquet type %s.", typeName), e);
            }
        } else if (repetitionCount == 0) {
            // For non-REPEATED type, repetitionCount can only be 0 or 1
            // repetitionCount == 0 means this is a null LIST/Primitive
            field.type = converter.getDataType().getOID();
            field.val = null;
        } else {
            // repetitionCount can only be 1
            field.type = converter.getDataType().getOID();
            field.val = converter.read(group, columnIndex, 0);
        }
        return field;
    }
}
