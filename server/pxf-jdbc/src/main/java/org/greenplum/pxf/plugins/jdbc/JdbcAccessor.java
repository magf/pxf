package org.greenplum.pxf.plugins.jdbc;

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

import io.arenadata.security.encryption.client.service.DecryptClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.security.SecureLogin;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.jdbc.utils.ConnectionManager;
import org.greenplum.pxf.plugins.jdbc.writercallable.WriterCallable;
import org.greenplum.pxf.plugins.jdbc.writercallable.WriterCallableFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * JDBC tables accessor
 * <p>
 * The SELECT queries are processed by {@link java.sql.Statement}
 * <p>
 * The INSERT queries are processed by {@link java.sql.PreparedStatement} and
 * built-in JDBC batches of arbitrary size
 */
public class JdbcAccessor extends JdbcBasePlugin implements Accessor {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcAccessor.class);

    private static final String JDBC_READ_PREPARED_STATEMENT_PROPERTY_NAME = "jdbc.read.prepared-statement";

    private Statement statementRead = null;
    private ResultSet resultSetRead = null;

    private WriterCallableFactory writerCallableFactory = null;
    private WriterCallable writerCallable = null;
    private ExecutorService executorServiceWrite = null;
    private Semaphore semaphore;
    private ConcurrentLinkedQueue<Future<SQLException>> poolTasks;
    private AtomicReference<Exception> firstException;

    /**
     * Creates a new instance of the JdbcAccessor
     */
    public JdbcAccessor() {
        super();
    }

    /**
     * Creates a new instance of accessor with provided connection manager.
     *
     * @param connectionManager connection manager
     * @param secureLogin       the instance of the secure login
     */
    JdbcAccessor(ConnectionManager connectionManager, SecureLogin secureLogin, DecryptClient decryptClient) {
        super(connectionManager, secureLogin, decryptClient);
    }

    /**
     * openForRead() implementation
     * Create query, open JDBC connection, execute query and store the result into resultSet
     *
     * @return true if successful
     * @throws SQLException        if a database access error occurs
     * @throws SQLTimeoutException if a problem with the connection occurs
     */
    @Override
    public boolean openForRead() throws SQLException, SQLTimeoutException {
        if (statementRead != null && !statementRead.isClosed()) {
            return true;
        }

        Connection connection = getConnection();
        try {
            return openForReadInner(connection);
        } catch (Throwable e) {
            if (statementRead == null) {
                closeConnection(connection);
            }
            throw new PxfRuntimeException(e.getMessage(), e);
        }
    }

    private boolean openForReadInner(Connection connection) throws SQLException {
        SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(context, connection.getMetaData(), getQueryText());

        // Build SELECT query
        if (quoteColumns == null) {
            sqlQueryBuilder.autoSetQuoteString();
        } else if (quoteColumns) {
            sqlQueryBuilder.forceSetQuoteString();
        }

        if (wrapDateWithTime) {
            sqlQueryBuilder.setWrapDateWithTime(true);
        }

        // Read variables
        String queryRead = sqlQueryBuilder.buildSelectQuery();
        LOG.trace("Select query: {}", queryRead);

        // Execute queries
        // Certain features of third-party JDBC drivers may require the use of a PreparedStatement, even if there are no

        // bind parameters. For example, Teradata's FastExport only works with PreparedStatements
        // https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BGBFBBEG
        boolean usePreparedStatement = parseJdbcUsePreparedStatementProperty();
        if (usePreparedStatement) {
            LOG.debug("Using a PreparedStatement instead of a Statement because {} was set to true", JDBC_READ_PREPARED_STATEMENT_PROPERTY_NAME);
        }
        statementRead = usePreparedStatement ?
                connection.prepareStatement(queryRead) :
                connection.createStatement();

        statementRead.setFetchSize(fetchSize);

        if (queryTimeout != null) {
            LOG.debug("Setting query timeout to {} seconds", queryTimeout);
            statementRead.setQueryTimeout(queryTimeout);
        }

        resultSetRead = usePreparedStatement ?
                ((PreparedStatement) statementRead).executeQuery() :
                statementRead.executeQuery(queryRead);

        return true;
    }

    /**
     * readNextObject() implementation
     * Retreive the next tuple from resultSet and return it
     *
     * @return row
     * @throws SQLException if a problem in resultSet occurs
     */
    @Override
    public OneRow readNextObject() throws SQLException {
        if (resultSetRead.next()) {
            return new OneRow(resultSetRead);
        }
        return null;
    }

    /**
     * closeForRead() implementation
     */
    @Override
    public void closeForRead() throws SQLException {
        closeStatementAndConnection(statementRead);
    }

    /**
     * openForWrite() implementation
     * Create query template and open JDBC connection
     *
     * @return true if successful
     * @throws SQLException        if a database access error occurs
     * @throws SQLTimeoutException if a problem with the connection occurs
     */
    @Override
    public boolean openForWrite() throws SQLException, SQLTimeoutException {
        if (queryName != null) {
            throw new IllegalArgumentException("specifying query name in data path is not supported for JDBC writable external tables");
        }

        Connection connection = getConnection();
        SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(context, connection.getMetaData());

        // Build INSERT query
        if (quoteColumns == null) {
            sqlQueryBuilder.autoSetQuoteString();
        } else if (quoteColumns) {
            sqlQueryBuilder.forceSetQuoteString();
        }
        // Write variables
        String queryWrite = sqlQueryBuilder.buildInsertQuery();
        LOG.trace("Insert query: {}", queryWrite);

        // Process batchSize
        if (!connection.getMetaData().supportsBatchUpdates()) {
            if ((batchSizeIsSetByUser) && (batchSize > 1)) {
                throw new SQLException("The external database does not support batch updates");
            } else {
                batchSize = 1;
            }
        }

        // Process poolSize
        if (poolSize < 1) {
            poolSize = Runtime.getRuntime().availableProcessors();
            LOG.info("The POOL_SIZE is set to the number of CPUs available ({})", poolSize);
        }
        executorServiceWrite = Executors.newFixedThreadPool(poolSize);
        semaphore = new Semaphore(poolSize);
        poolTasks = new ConcurrentLinkedQueue<>();
        firstException = new AtomicReference<>();

        // Setup WriterCallableFactory
        writerCallableFactory = new WriterCallableFactory(this, queryWrite, batchSize, () -> semaphore.release());
        writerCallable = writerCallableFactory.get();

        closeConnection(connection);
        return true;
    }

    /**
     * writeNextObject() implementation
     * <p>
     * If batchSize is not 0 or 1, add a tuple to the batch of statementWrite
     * Otherwise, execute an INSERT query immediately
     * <p>
     * In both cases, a {@link java.sql.PreparedStatement} is used
     *
     * @param row one row
     * @return true if successful
     * @throws SQLException           if a database access error occurs
     * @throws IOException            if the data provided by {@link JdbcResolver} is corrupted
     * @throws ClassNotFoundException if pooling is used and the JDBC driver was not found
     * @throws IllegalStateException  if writerCallableFactory was not properly initialized
     * @throws Exception              if it happens in writerCallable.call()
     */
    @Override
    public boolean writeNextObject(OneRow row) throws Exception {
        if (writerCallable == null) {
            throw new IllegalStateException("The JDBC connection was not properly initialized (writerCallable is null)");
        }

        writerCallable.supply(row);
        if (writerCallable.isCallRequired()) {
            // Semaphore#release runs as onComplete.run() in a 'finally' statement of WriterCallable#call
            semaphore.acquire();
            Future<SQLException> future = executorServiceWrite.submit(writerCallable);
            poolTasks.add(future);
            writerCallable = writerCallableFactory.get();
            // Check results for tasks that has already done
            checkWriteNextObjectResults();
        }

        return true;
    }

    /**
     * closeForWrite() implementation
     *
     * @throws Exception if it happens in writerCallable.call() or due to runtime errors in thread pool
     */
    @Override
    public void closeForWrite() throws Exception {
        if (writerCallable == null) {
            return;
        }

        if (firstException.get() == null) {
            if (writerCallable != null) {
                // Send data that is left
                Future<SQLException> f = executorServiceWrite.submit(writerCallable);
                poolTasks.add(f);
                LOG.debug("Writer {} dumps last batch with the future {}", writerCallable, f);
                checkCloseForWriteResults();
                shutdownExecutorService();
            }
        } else {
            shutdownExecutorService();
            throw firstException.get();
        }
    }

    private void checkWriteNextObjectResults() throws Exception {
        Exception e;
        for (Future<SQLException> f : poolTasks) {
            if (f.isDone()) {
                try {
                    e = f.get();
                    LOG.debug("Writer {} completed inserting batch with the future {}", writerCallable, f);
                } catch (Exception ex) {
                    e = ex;
                }
                poolTasks.remove(f);
                if (e != null) {
                    throwException(e);
                }
            }
        }
    }

    private void checkCloseForWriteResults() throws Exception {
        Exception e;
        for (Future<SQLException> f : poolTasks) {
            try {
                e = f.get();
                LOG.debug("Writer {} completed inserting batch with the future {}", writerCallable, f);
            } catch (Exception ex) {
                e = ex;
            }
            if (e != null) {
                throwException(e);
            }
        }
        poolTasks.clear();
    }

    private void throwException(Exception e) throws Exception {
        firstException.compareAndSet(null, e);
        String msg = e.getMessage();
        if (msg == null)
            msg = e.getClass().getName();
        LOG.error("A writer completed inserting batch with exception: {}", msg);
        throw e;
    }

    private void shutdownExecutorService() {
        LOG.debug("Start shutdown executor service for write");
        executorServiceWrite.shutdownNow();
        try {
            if (!executorServiceWrite.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("Executor did not terminate in the specified time.");
                List<Runnable> droppedTasks = executorServiceWrite.shutdownNow();
                LOG.warn("Dropped " + droppedTasks.size() + " tasks");
            }
        } catch (InterruptedException e) {
            LOG.warn("Thread received interrupted signal");
            Thread.currentThread().interrupt();
        }
        LOG.info("Shutdown executor service completed");
    }


    /**
     * Gets the text of the query by reading the file from the server configuration directory. The name of the file
     * is expected to be the same as the name of the query provided by the user and have extension ".sql"
     *
     * @return text of the query
     */
    private String getQueryText() {
        if (StringUtils.isBlank(queryName)) {
            return null;
        }
        // read the contents of the file holding the text of the query with a given name
        String serverDirectory = context.getConfiguration().get(ConfigurationFactory.PXF_CONFIG_SERVER_DIRECTORY_PROPERTY);
        if (StringUtils.isBlank(serverDirectory)) {
            throw new IllegalStateException("No server configuration directory found for server " + context.getServerName());
        }

        String queryText;
        try {
            File queryFile = new File(serverDirectory, queryName + ".sql");
            LOG.debug("Reading text of query={} from {}", queryName, queryFile.getCanonicalPath());
            queryText = FileUtils.readFileToString(queryFile, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to read text of query %s : %s", queryName, e.getMessage()), e);
        }
        if (StringUtils.isBlank(queryText)) {
            throw new RuntimeException(String.format("Query text file is empty for query %s", queryName));
        }

        // Remove one or more semicolons followed by optional blank space
        // happening at the end of the query
        queryText = queryText.replaceFirst("(;+\\s*)+$", "");

        return queryText;
    }

    private boolean parseJdbcUsePreparedStatementProperty() {
        return Utilities.parseBooleanProperty(configuration, JDBC_READ_PREPARED_STATEMENT_PROPERTY_NAME, false);
    }
}
