package org.greenplum.pxf.automation.arenadata;

import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.cluster.MultiNodeCluster;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.SegmentNode;
import org.greenplum.pxf.automation.components.oracle.Oracle;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.greenplum.pxf.automation.PxfTestConstant.*;
import static org.junit.Assert.assertEquals;

public class PushdownPredicateInTest extends BaseFeature {
    private static final String SOURCE_TABLE_NAME = "predicate_in_source_table";
    private static final String SOURCE_TABLE_SCHEMA = "system";
    private static final String PXF_ORACLE_SERVER_PROFILE = "oracle-predicate";
    private static final String PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH = "templates/oracle/jdbc-site.xml";
    private static final String PXF_APP_PROPERTIES_RELATIVE_PATH = "conf/pxf-application.properties";
    private static final String PXF_LOG_RELATIVE_PATH = "logs/pxf-service.log";
    private static final String PXF_TEMP_LOG_PATH = "/tmp/pxf-service.log";
    private static final String[] POSTGRES_SOURCE_TABLE_FIELDS = new String[]{
            "id    int",
            "descr   text"};
    private static final String[] ORACLE_SOURCE_TABLE_FIELDS = new String[]{
            "id    NUMBER",
            "descr   VARCHAR2(100)"};

    private static final String ORACLE_INSERT_QUERY = "INSERT ALL\n" +
            "INTO system.predicate_in_source_table VALUES (1, 'text1')\n" +
            "INTO system.predicate_in_source_table VALUES (2, 'text2')\n" +
            "INTO system.predicate_in_source_table VALUES (3, 'text3')\n" +
            "INTO system.predicate_in_source_table VALUES (4, 'text4')\n" +
            "INTO system.predicate_in_source_table VALUES (5, 'text5')\n" +
            "SELECT 1 FROM DUAL";
    private static final String GET_LATEST_MASTER_LOG_COMMAND = "$(stat -c '%Y %n' /data1/master/gpseg-1/pg_log/gpdb-*.csv " +
            "| sort -k1,1nr | head -1 | awk '{ print $2 }')";
    private static final String POSTGRES_SEGMENT_LOG_GREP_COMMAND = "cat " + PXF_TEMP_LOG_PATH + " | grep ' FILTER target:  WHERE id IN (2,3)' | wc -l";
    private static final String GET_STATS_QUERY = "SELECT count(*) FROM v$sqlstats " +
            "WHERE SQL_FULLTEXT LIKE 'SELECT id, descr FROM " + SOURCE_TABLE_SCHEMA + "." + SOURCE_TABLE_NAME + " WHERE id IN (3,4,5)'";
    private Oracle oracle;
    private Node pxfNode;
    private String pxfHome;
    private String pxfJdbcSiteConfFile;
    private String pxfLogFile;
    private Table gpdbPredicateInSourceTable;
    private Table oraclePredicateInSourceTable;

    @Override
    protected void beforeClass() throws Exception {
        pxfHome = cluster.getPxfHome();
        String pxfJdbcSiteConfPath = String.format(PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE, pxfHome, PXF_ORACLE_SERVER_PROFILE);
        pxfJdbcSiteConfFile = pxfJdbcSiteConfPath + "/" + PXF_JDBC_SITE_CONF_FILE_NAME;
        String pxfJdbcSiteConfTemplate = pxfHome + "/" + PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH;
        String pxfAppPropertyFile = pxfHome + "/" + PXF_APP_PROPERTIES_RELATIVE_PATH;
        cluster.copyFileToNodes(pxfJdbcSiteConfTemplate, pxfJdbcSiteConfPath, true, false);
        cluster.runCommandOnAllNodes("sed -i 's/# pxf.log.level=info/pxf.log.level=debug/' " + pxfAppPropertyFile);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        if (cluster instanceof MultiNodeCluster) {
            pxfNode = ((MultiNodeCluster) cluster).getNode(SegmentNode.class, PhdCluster.EnumClusterServices.pxf).get(0);
        }
        pxfLogFile = pxfHome + "/" + PXF_LOG_RELATIVE_PATH;
        oracle = (Oracle) SystemManagerImpl.getInstance().getSystemObject("oracle");
        prepareData();
    }

    protected void prepareData() throws Exception {
        preparePostgresPredicateInSourceTable();
        prepareOraclePredicateInSourceTable();
        createGpdbReadablePgTable();
        createGpdbReadableOracleTable();
    }

    private void preparePostgresPredicateInSourceTable() throws Exception {
        gpdbPredicateInSourceTable = new Table(SOURCE_TABLE_NAME, POSTGRES_SOURCE_TABLE_FIELDS);
        gpdbPredicateInSourceTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(gpdbPredicateInSourceTable);
        String[][] rows = new String[][]{
                {"1", "text1"},
                {"2", "text2"},
                {"3", "text3"},
                {"4", "text4"},
                {"5", "text5"}};
        Table dataTable = new Table("dataTable", POSTGRES_SOURCE_TABLE_FIELDS);
        dataTable.addRows(rows);
        gpdb.insertData(dataTable, gpdbPredicateInSourceTable);
    }

    private void prepareOraclePredicateInSourceTable() throws Exception {
        oraclePredicateInSourceTable = new Table(SOURCE_TABLE_NAME, ORACLE_SOURCE_TABLE_FIELDS);
        oraclePredicateInSourceTable.setSchema("system");
        oracle.createTableAndVerify(oraclePredicateInSourceTable);
        oracle.runQuery(ORACLE_INSERT_QUERY);
    }

    private void createGpdbReadablePgTable() throws Exception {
        ExternalTable gpdbReadablePgTable = TableFactory.getPxfJdbcReadableTable(
                "predicate_in_pg_ext_table",
                POSTGRES_SOURCE_TABLE_FIELDS,
                gpdbPredicateInSourceTable.getName(),
                "default");
        gpdb.createTableAndVerify(gpdbReadablePgTable);
    }

    private void createGpdbReadableOracleTable() throws Exception {
        ExternalTable gpdbReadableOracleTable = TableFactory.getPxfJdbcReadableTable(
                "predicate_in_oracle_ext_table",
                POSTGRES_SOURCE_TABLE_FIELDS,
                oraclePredicateInSourceTable.getSchema() + "." + oraclePredicateInSourceTable.getName(),
                PXF_ORACLE_SERVER_PROFILE);
        gpdb.createTableAndVerify(gpdbReadableOracleTable);
    }

    @Test(groups = {"arenadata"}, description = "Check pushdown predicate 'IN' for Postres")
    public void testPredicateInPostgres() throws Exception {
        // Clean gpdb master log and pxf log before run test
        cluster.runCommand("> " + GET_LATEST_MASTER_LOG_COMMAND);
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
        runSqlTest("pxf.arenadata.predicate-in.postgres.runTest");
        cluster.runCommand("grep -e 'SELECT id, descr FROM " + SOURCE_TABLE_NAME + " WHERE id IN (2,3)' " + GET_LATEST_MASTER_LOG_COMMAND + " | wc -l");
        String result = cluster.getLastCmdResult();
        String[] results = result.split("\r\n");
        String actualExitCode = results.length > 1 ? results[1].trim() : "Exit code is empty";
        assertEquals("1", actualExitCode);
    }

    @Test(groups = {"arenadata"}, dependsOnMethods = {"testPredicateInPostgres"}, description = "Check pushdown predicate 'IN' logging")
    public void testPredicateInLogging() throws Exception {
        cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, "/tmp/");
        cluster.runCommand(POSTGRES_SEGMENT_LOG_GREP_COMMAND);
        String result = cluster.getLastCmdResult();
        String[] results = result.split("\r\n");
        String actualExitCode = results.length > 1 ? results[1].trim() : "Exit code is empty";
        assertEquals("1", actualExitCode);
        cluster.deleteFileFromNodes(PXF_TEMP_LOG_PATH, false);
    }

    @Test(groups = {"arenadata"}, description = "Check pushdown predicate 'IN' for Oracle")
    public void testPredicateInOracle() throws Exception {
        runSqlTest("pxf.arenadata.predicate-in.oracle.runTest");
        Assert.assertEquals(1, oracle.getValueFromQuery(GET_STATS_QUERY));
        cluster.deleteFileFromNodes(pxfJdbcSiteConfFile, false);
    }
}
