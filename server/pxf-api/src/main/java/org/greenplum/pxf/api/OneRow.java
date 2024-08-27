package org.greenplum.pxf.api;

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


import lombok.Getter;
import lombok.Setter;

/**
 * Represents one row in the external system data store.
 * Supports the general case where one row contains both a record and a
 * separate key like in the HDFS key/value model for MapReduce (Example: HDFS sequence file).
 */
@Setter
@Getter
public class OneRow {
    private Object key;
    private Object data;

    public OneRow(){
    }

    /**
     * Constructs a OneRow
     *
     * @param key the key for the record
     * @param data the actual record
     */
    public OneRow(Object key, Object data) {
        this.key = key;
        this.data = data;
    }

    /**
     * Constructs a OneRow without a key value
     *
     * @param data the actual record
     */
    public OneRow(Object data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "OneRow:" + key + "->" + data;
    }
}

