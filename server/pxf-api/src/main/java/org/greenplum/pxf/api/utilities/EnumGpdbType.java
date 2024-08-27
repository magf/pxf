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

package org.greenplum.pxf.api.utilities;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Getter;
import org.greenplum.pxf.api.io.DataType;

import java.io.IOException;

class EnumGpdbTypeSerializer extends JsonSerializer<EnumGpdbType> {

    @Override
    public void serialize(EnumGpdbType value, JsonGenerator generator,
                          SerializerProvider provider) throws IOException {
      generator.writeString(value.getTypeName());
    }
  }

/**
 * 
 * GPDB types which could be used in plugins.
 *
 */
@Getter
@JsonSerialize(using = EnumGpdbTypeSerializer.class)
public enum EnumGpdbType {
    Int2Type("int2", DataType.SMALLINT),
    Int4Type("int4", DataType.INTEGER),
    Int8Type("int8", DataType.BIGINT),
    Float4Type("float4", DataType.REAL),
    Float8Type("float8", DataType.FLOAT8),
    TextType("text", DataType.TEXT),
    VarcharType("varchar", DataType.VARCHAR, (byte) 1),
    ByteaType("bytea", DataType.BYTEA),
    DateType("date", DataType.DATE),
    TimestampType("timestamp", DataType.TIMESTAMP),
    BoolType("bool", DataType.BOOLEAN),
    NumericType("numeric", DataType.NUMERIC, (byte) 2),
    BpcharType("bpchar", DataType.BPCHAR, (byte) 1);

    private final DataType dataType;
    private final String typeName;
    private byte modifiersNum;

    EnumGpdbType(String typeName, DataType dataType) {
        this.typeName = typeName;
        this.dataType = dataType;
    }

    EnumGpdbType(String typeName, DataType dataType, byte modifiersNum) {
        this(typeName, dataType);
        this.modifiersNum = modifiersNum;
    }
}
