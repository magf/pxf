package org.greenplum.pxf.plugins.jdbc.writercallable;

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

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.plugins.jdbc.JdbcBasePlugin;
import org.greenplum.pxf.plugins.jdbc.JdbcResolver;

import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This writer makes batch INSERTs.
 * A call() is required after a certain number of supply() calls
 */
class BatchWriterCallable implements WriterCallable {
    private final JdbcBasePlugin plugin;
    private final String query;
    private final List<OneRow> rows;
    private final int batchSize;

    /**
     * Construct a new batch writer
     */
    BatchWriterCallable(JdbcBasePlugin plugin, String query, int batchSize) {
        if (plugin == null || query == null) {
            throw new IllegalArgumentException("The provided JdbcBasePlugin or SQL query is null");
        }

        this.plugin = plugin;
        this.query = query;
        this.batchSize = batchSize;
        rows = new ArrayList<>();
    }

    @Override
    public void supply(OneRow row) throws IllegalStateException {
        if ((batchSize > 0) && (rows.size() >= batchSize)) {
            throw new IllegalStateException("Trying to supply() a OneRow object to a full WriterCallable");
        }
        if (row == null) {
            throw new IllegalArgumentException("Trying to supply() a null OneRow object");
        }
        rows.add(row);
    }

    @Override
    public boolean isCallRequired() {
        return (batchSize > 0) && (rows.size() >= batchSize);
    }

    @Override
    public SQLException call() throws IOException, SQLException {
        if (rows.isEmpty()) {
            return null;
        }

        PreparedStatement statement = null;
        try {
            statement = plugin.getPreparedStatement(plugin.getConnection(), query);
            for (OneRow row : rows) {
                JdbcResolver.decodeOneRowToPreparedStatement(row, statement);
                statement.addBatch();
            }

            statement.executeBatch();
        } catch (BatchUpdateException bue) {
            SQLException cause = bue.getNextException();
            return cause != null ? cause : bue;
        } catch (SQLException e) {
            return e;
        } finally {
            rows.clear();
            JdbcBasePlugin.closeStatementAndConnection(statement);
        }

        return null;
    }
}
