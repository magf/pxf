package org.greenplum.pxf.automation.arenadata;

import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.cluster.MultiNodeCluster;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.SegmentNode;
import org.greenplum.pxf.automation.components.common.cli.ShellCommandErrorException;
import org.greenplum.pxf.automation.components.oracle.Oracle;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;

import static org.greenplum.pxf.automation.PxfTestConstant.*;

public class JdbcBackPressureTest extends BaseFeature {
    private static final String PXF_SERVER_PROFILE = "backpressure";
    private static final String PXF_APP_PROPERTIES_FILE_RELATIVE_PATH = "conf/pxf-application.properties";
    private static final String PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH = "templates/backpressure/jdbc-site.xml";
    private static final String PXF_TEMP_LOG_PATH = "/tmp/pxf-service.log";
    private static final String[] ORACLE_TARGET_TABLE_FIELDS = new String[]{
            "id1    NUMBER",
            "id2    NUMBER",
            "gen    VARCHAR2(100)",
            "now    TIMESTAMP(6)"};

    private static final String[] GPDB_SOURCE_TABLE_FIELDS = new String[]{
            "id1    int",
            "id2    int",
            "gen    text",
            "now    timestamp without time zone"};

    private static final String INSERT_QUERY = "INSERT INTO gp_source_table\n" +
            "SELECT gen, gen, 'text' || gen::text, now() FROM generate_series (1,8000000) gen;";

    // Sometimes the queue in the thread pool might be more than 0 for some reason:
    // 1. The semaphore is released a slightly early then the task is removed from the active tasks in the pool;
    // 2. The logging is not atomic operation
    // The output of the grep command is the number of lines with the queue less than 3. Should be ROWS_COUNT/BATCH_SIZE - 1
    private static final String QUEUE_USED_GREP_COMMAND = "cat " + PXF_TEMP_LOG_PATH + " | grep 'Current thread pool active task' " +
            "| awk -F 'Queue used: ' '{print $2}' | awk -F ';' '{ if ( $1+0 <= 3 ) print $1 }' | wc -l";

    // Current thread pool active task cannot be more than POOL_SIZE value
    // The output of the grep command is the number of lines with the active task less than %d. Should be ROWS_COUNT/BATCH_SIZE - 1
    private static final String POOL_ACTIVE_TASK_GREP_COMMAND_TEMPLATE = "cat " + PXF_TEMP_LOG_PATH + " | grep 'Current thread pool active task' " +
            "| awk -F 'Current thread pool active task: ' '{print $2}' | awk -F ';' '{ if ( $1+0 <= %d ) print $1 }' | wc -l";

    // Semaphore remains cannot be more than POOL_SIZE value
    // The output of the grep command is the number of lines with the semaphore remains less than %d. Should be ROWS_COUNT/BATCH_SIZE - 1
    private static final String SEMAPHORE_REMAINS_GREP_COMMAND_TEMPLATE = "cat " + PXF_TEMP_LOG_PATH + " | grep 'Current thread pool active task' " +
            "| awk -F 'Semaphore remains: ' '{print $2}' | awk -F ';' '{ if ( $1+0 <= %d ) print $1 }' | wc -l";

    private Table oracleTargetTable;
    private String pxfHome;
    private String pxfJdbcSiteConfPath;
    private String pxfJdbcSiteConfFile;
    private String pxfJdbcSiteConfTemplate;
    private String pxfLogFile;
    private Node pxfNode;
    private Oracle oracle;

    @Override
    public void beforeClass() throws Exception {
        pxfHome = cluster.getPxfHome();
        pxfJdbcSiteConfPath = String.format(PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE, pxfHome, PXF_SERVER_PROFILE);
        pxfJdbcSiteConfFile = pxfJdbcSiteConfPath + "/" + PXF_JDBC_SITE_CONF_FILE_NAME;
        pxfJdbcSiteConfTemplate = pxfHome + "/" + PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH;
        pxfLogFile = pxfHome + "/" + PXF_LOG_RELATIVE_PATH;
        if (cluster instanceof MultiNodeCluster) {
            pxfNode = ((MultiNodeCluster) cluster).getNode(SegmentNode.class, PhdCluster.EnumClusterServices.pxf).get(0);
        }
        oracle = (Oracle) SystemManagerImpl.getInstance().getSystemObject("oracle");
        changeLogLevel();
        prepareData();
    }

    protected void prepareData() throws Exception {
        // Greenplum internal source table
        createGpdbSourceTable();
        // Oracle internal target table
        prepareOracleTargetTable();
        // External writable table to run query when jdbc.pool.property.maximumPoolSize = 1
        createGpdbWritableTable("jdbc_bp_write_connection_max_pool_size_1", "POOL_SIZE=4,BATCH_SIZE=20000");
        // External writable table to run query when POOL_SIZE = 1
        createGpdbWritableTable("jdbc_bp_write_pool_size_1", "POOL_SIZE=1,BATCH_SIZE=20000");
        // External writable table to run query when POOL_SIZE = 10
        createGpdbWritableTable("jdbc_bp_write_pool_size_10", "POOL_SIZE=10,BATCH_SIZE=3000");
        // External writable table to run query when BATCH_TIMEOUT = 2 seconds
        createGpdbWritableTable("jdbc_bp_write_batch_timeout_error", "POOL_SIZE=1,BATCH_SIZE=1000000,BATCH_TIMEOUT=2");
        // External writable table to run query when BATCH_TIMEOUT = 120 seconds
        createGpdbWritableTable("jdbc_bp_write_batch_timeout_success", "POOL_SIZE=1,BATCH_SIZE=1000000,BATCH_TIMEOUT=120");
    }

    private void createGpdbSourceTable() throws Exception {
        Table gpdbSourceTable = new Table("gp_source_table", GPDB_SOURCE_TABLE_FIELDS);
        gpdbSourceTable.setDistributionFields(new String[]{"id1"});
        gpdb.createTableAndVerify(gpdbSourceTable);
        gpdb.runQuery(INSERT_QUERY);
    }

    private void prepareOracleTargetTable() throws Exception {
        oracleTargetTable = new Table("oracle_target_table", ORACLE_TARGET_TABLE_FIELDS);
        oracleTargetTable.setSchema("system");
        oracle.createTableAndVerify(oracleTargetTable);
    }

    private void createGpdbWritableTable(String tableName, String customParams) throws Exception {
        ExternalTable gpdbWritableTable = TableFactory.getPxfWritableCustomTable(
                tableName,
                GPDB_SOURCE_TABLE_FIELDS,
                oracleTargetTable.getSchema() + "." + oracleTargetTable.getName());
        gpdbWritableTable.setProfile("jdbc");
        gpdbWritableTable.setServer("SERVER=" + PXF_SERVER_PROFILE);
        gpdbWritableTable.setUserParameters(customParams.split(","));
        gpdb.createTableAndVerify(gpdbWritableTable);
    }

    @Test(groups = {"arenadata"}, description = "Check that jdbc.pool.property.maximumPoolSize = 1 is enough for success query")
    public void checkConnectionPoolWithMaxPoolSizeIs1() throws Exception {
        copyJdbcConfFile(pxfJdbcSiteConfTemplate);
        modifyJdbcConfFile("'s/<value>10<\\/value>/<value>1<\\/value>/g'");
        runSqlTest("arenadata/backpressure/connection-max-poolsize-1");
    }

    @Test(groups = {"arenadata"}, description = "Check back-pressure when POOL_SIZE=1")
    public void checkBackPressureWhenPoolSizeIs1() throws Exception {
        cleanPxfLog();
        copyJdbcConfFile(pxfJdbcSiteConfTemplate);
        runSqlTest("arenadata/backpressure/poolsize-1");
        copyPxfLog();
        Assert.assertEquals("Check queue used when POOL_SIZE=1",
                grepPxfLog(QUEUE_USED_GREP_COMMAND), "49");
        Assert.assertEquals("Check pool active tasks when POOL_SIZE=1",
                grepPxfLog(String.format(POOL_ACTIVE_TASK_GREP_COMMAND_TEMPLATE, 1)), "49");
        Assert.assertEquals("Check semaphore remains when POOL_SIZE=1",
                grepPxfLog(String.format(SEMAPHORE_REMAINS_GREP_COMMAND_TEMPLATE, 1)), "49");
    }

    @Test(groups = {"arenadata"}, description = "Check back-pressure when POOL_SIZE=10")
    public void checkBackPressureWhenPoolSizeIs10() throws Exception {
        cleanPxfLog();
        copyJdbcConfFile(pxfJdbcSiteConfTemplate);
        modifyJdbcConfFile("'s/<value>10<\\/value>/<value>2<\\/value>/g'");
        runSqlTest("arenadata/backpressure/poolsize-10");
        copyPxfLog();
        Assert.assertEquals("Check queue used when POOL_SIZE=10",
                grepPxfLog(QUEUE_USED_GREP_COMMAND), "999");
        Assert.assertEquals("Check pool active tasks when POOL_SIZE=10",
                grepPxfLog(String.format(POOL_ACTIVE_TASK_GREP_COMMAND_TEMPLATE, 10)), "999");
        Assert.assertEquals("Check semaphore remains when POOL_SIZE=10",
                grepPxfLog(String.format(SEMAPHORE_REMAINS_GREP_COMMAND_TEMPLATE, 10)), "999");
    }

    @Test(groups = {"arenadata"}, description = "Check parameter BATCH_TIMEOUT when query finished with error")
    public void checkBatchTimeoutError() throws Exception {
        copyJdbcConfFile(pxfJdbcSiteConfTemplate);
        runSqlTest("arenadata/backpressure/batch-timeout/error");
    }

    @Test(groups = {"arenadata"}, description = "Check parameter BATCH_TIMEOUT when query finished with success")
    public void checkBatchTimeoutSuccess() throws Exception {
        copyJdbcConfFile(pxfJdbcSiteConfTemplate);
        runSqlTest("arenadata/backpressure/batch-timeout/success");
    }

    private void changeLogLevel() throws Exception {
        String pxfAppPropertiesFile = cluster.getPxfHome() + "/" + PXF_APP_PROPERTIES_FILE_RELATIVE_PATH;
        cluster.runCommandOnAllNodes("sed -i 's/# pxf.log.level=info/pxf.log.level=trace/' " + pxfAppPropertiesFile);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
    }

    private void cleanPxfLog() throws Exception {
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
    }

    private void copyPxfLog() throws Exception {
        cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, "/tmp/");
    }

    private String grepPxfLog(String command) throws ShellCommandErrorException, IOException {
        cluster.runCommand(command);
        String result = cluster.getLastCmdResult();
        String[] results = result.split("\r\n");
        return results.length > 1 ? results[1].trim() : "Result is empty";
    }

    private void copyJdbcConfFile(String templateSource) throws Exception {
        cluster.deleteFileFromNodes(pxfJdbcSiteConfFile, false);
        cluster.copyFileToNodes(templateSource, pxfJdbcSiteConfPath, true, false);
    }

    private void modifyJdbcConfFile(String sedExpression) throws Exception {
        cluster.runCommandOnAllNodes("sed -i " + sedExpression + " " + pxfJdbcSiteConfFile);
    }
}
