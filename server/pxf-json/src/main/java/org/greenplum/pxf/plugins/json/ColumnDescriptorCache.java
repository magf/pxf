package org.greenplum.pxf.plugins.json;

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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.Getter;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

/**
 * Helper class used to retrieve all column details relevant for the json processing.
 */
public class ColumnDescriptorCache {

	private static final Pattern ARRAY_PROJECTION_PATTERN = Pattern.compile("(.+)\\[([0-9]+)\\]");
	private static final int ARRAY_NAME_GROUPID = 1;
	private static final int ARRAY_INDEX_GROUPID = 2;
    @Getter
    private final DataType columnType;
	private final String[] normalizedProjection;
    @Getter
    private final int arrayNodeIndex;
	private final boolean isArray;
    @Getter
    private final String columnName;

	public ColumnDescriptorCache(ColumnDescriptor columnDescriptor) {

		// Greengage column type
		this.columnType = columnDescriptor.getDataType();

		this.columnName = columnDescriptor.columnName();

		// Column name can use dot-name convention to specify a nested json node.
		// Break the path into array of path steps called projections
		String[] projection = columnDescriptor.columnName().split("\\.");

		// When the projection contains array reference (e.g. projections = foo.bar[66]) then replace the last path
		// element by the array name (e.g. normalizedProjection = foo.bar)
		normalizedProjection = new String[projection.length];

		// Check if the provided json path (projections) refers to an array element.
		Matcher matcher = ARRAY_PROJECTION_PATTERN.matcher(projection[projection.length - 1]);
		if (matcher.matches()) {
			this.isArray = true;
			// extracts the array node name from the projection path
			String arrayNodeName = matcher.group(ARRAY_NAME_GROUPID);
			// extracts the array index from the projection path
			this.arrayNodeIndex = Integer.parseInt(matcher.group(ARRAY_INDEX_GROUPID));

			System.arraycopy(projection, 0, normalizedProjection, 0, projection.length - 1);
			normalizedProjection[projection.length - 1] = arrayNodeName;
		} else {
			this.isArray = false;
			this.arrayNodeIndex = -1;

			System.arraycopy(projection, 0, normalizedProjection, 0, projection.length);
		}
	}

    /**
	 * If the column name contains dots (.) then this name is interpreted as path into the target json document pointing
	 * to nested json member. The leftmost path element stands for the root in the json document.
	 * 
	 * @return If the column name contains dots (.) list of field names that represent the path from the root json node
	 *         to the target nested node.
	 */
	public String[] getNormalizedProjections() {
		return normalizedProjection;
	}

    /**
	 * @return Returns true if the column name is a path to json array element and false otherwise.
	 */
	public boolean isArray() {
		return isArray;
	}
}
