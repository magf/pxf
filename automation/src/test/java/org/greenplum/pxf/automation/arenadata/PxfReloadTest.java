package org.greenplum.pxf.automation.arenadata;

import io.qameta.allure.Feature;
import io.qameta.allure.Step;
import org.apache.commons.lang3.StringUtils;
import org.awaitility.Awaitility;
import org.greenplum.pxf.automation.components.cluster.MultiNodeCluster;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.CoordinatorNode;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.SegmentNode;
import org.greenplum.pxf.automation.components.common.cli.ShellCommandErrorException;
import org.greenplum.pxf.automation.datapreparer.CustomTextPreparer;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.fileformats.FileFormatsUtils;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.greenplum.pxf.automation.PxfTestConstant.PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE;
import static org.greenplum.pxf.automation.PxfTestConstant.PXF_LOG_RELATIVE_PATH;
import static org.greenplum.pxf.automation.PxfTestUtil.getCmdResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Feature("PXF reload")
public class PxfReloadTest extends BaseFeature {
    private static final int INIT_QUERY_DELAY = 15;
    private static final int INIT_QUERY_POLL_INTERVAL = 3;
    private static final String SUFFIX_CLASS = ".class";

    private static final String INSERT_EXT_QUERY_PART = "INSERT INTO write_ext_table";
    private static final String INSERT_INT_QUERY_PART_TEMPLATE = "INSERT INTO %s";
    private static final String SELECT_EXT_QUERY_PART = "SELECT md5(t1.name) FROM";
    private static final String SELECT_INT_QUERY_PART_TEMPLATE = "SELECT name FROM %s";
    private static final String SELECT_EXT_QUERY_HDFS_PART = "SELECT md5(t1.s1) FROM";

    private static final String PSQL_SELECT_PG_TEMPLATE = "psql -d pxfautomation -c \"SELECT md5(t1.name) FROM %s t1 JOIN %s t2 ON t1.name = t2.name;\" &";
    private static final String PSQL_INSERT_PG_TEMPLATE = "psql -d pxfautomation -c \"INSERT INTO %s SELECT i, md5(random()::text) FROM generate_series(1,8000000) i;\" &";
    private static final String PSQL_SELECT_HDFS_TEMPLATE = "psql -d pxfautomation -c \"SELECT md5(t1.s1) FROM %s t1 JOIN %s t2 ON t1.s1 = t2.s1;\" &";

    private static final String PG_STAT_ACTIVITY_COUNT_QUERY = "SELECT count(*) FROM pg_stat_activity \n" +
            "WHERE usename = 'gpadmin' \n" +
            "AND query NOT LIKE '%pg_stat_activity%'";
    private static final String PG_STAT_ACTIVITY_QUERY = "SELECT * FROM pg_stat_activity WHERE usename = 'gpadmin';";

    private static final String EXT_WRITE_TABLE_NAME = "write_ext_table";
    private static final String EXT_WRITE_TABLE_NAME_2 = "write_ext_table2";
    private static final String EXT_READ_TABLE_NAME = "read_ext_table";
    private static final String EXT_READ_TABLE_NAME_2 = "read_ext_table2";
    private static final String SOURCE_TABLE_NAME = "gpdb_source_table";
    private static final String SOURCE_TABLE_NAME_2 = "gpdb_source_table2";
    private static final String TARGET_TABLE_NAME = "gpdb_target_table";
    private static final String TARGET_TABLE_NAME_2 = "gpdb_target_table2";

    private static final String PXF_TEMP_LOG_PATH = "/tmp/pxf";
    private static final String PXF_TEMP_LOG_FILE = PXF_TEMP_LOG_PATH + "/pxf-service.log";
    private static final String PXF_RELOAD_SERVER_PROFILE = "reload";
    private static final String PXF_RELOAD_SECOND_SERVER_PROFILE = "reload-second";
    private static final String PXF_RELOAD_DEFAULT_PROFILE = "default";
    private static final String PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH = "templates/pxf-reload/jdbc-site.xml";
    private static final String GREP_COMMAND_TEMPLATE = "cat %s | grep \"%s\" | wc -l";
    private static final String[] TABLE_FIELDS = new String[]{
            "id  int",
            "name text"};

    ProtocolEnum protocol;
    Table dataTable = null;
    Table pgStatActivity;
    String hdfsFilePath = "";
    String testPackageLocation = "/org/greenplum/pxf/automation/testplugin/";
    String throwOn10000Accessor = "ThrowOn10000Accessor";
    private List<Node> pxfNodes;
    private Node masterNode;
    private String pxfLogFile;

    @Override
    protected void beforeClass() throws Exception {
        if (!FDWUtils.useFDW) {
            String pxfHome = cluster.getPxfHome();
            String pxfJdbcSiteConfTemplate = pxfHome + "/" + PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH;

            String pxfJdbcSiteConfPath = String.format(PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE, pxfHome, PXF_RELOAD_SERVER_PROFILE);
            cluster.copyFileToNodes(pxfJdbcSiteConfTemplate, pxfJdbcSiteConfPath, true, false);

            String pxfSecondJdbcSiteConfPath = String.format(PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE, pxfHome, PXF_RELOAD_SECOND_SERVER_PROFILE);
            cluster.copyFileToNodes(pxfJdbcSiteConfTemplate, pxfSecondJdbcSiteConfPath, true, false);

            if (cluster instanceof MultiNodeCluster) {
                pxfNodes = ((MultiNodeCluster) cluster).getNode(SegmentNode.class, PhdCluster.EnumClusterServices.pxf);
                masterNode = ((MultiNodeCluster) cluster).getNode(CoordinatorNode.class, PhdCluster.EnumClusterServices.pxf).get(0);
            }
            pxfLogFile = pxfHome + "/" + PXF_LOG_RELATIVE_PATH;
            //hdfs preparation
            String resourcePath = "target/classes" + testPackageLocation;
            String newPath = "/tmp/publicstage/pxf";
            cluster.copyFileToNodes(new File(resourcePath + throwOn10000Accessor
                    + SUFFIX_CLASS).getAbsolutePath(), newPath
                    + testPackageLocation, true, false);
            cluster.addPathToPxfClassPath(newPath);
            protocol = ProtocolUtils.getProtocol();
            cluster.runCommand("mkdir -p " + PXF_TEMP_LOG_PATH);
            prepareSourceTable(SOURCE_TABLE_NAME);
            prepareSourceTable(SOURCE_TABLE_NAME_2);
            cluster.restart(PhdCluster.EnumClusterServices.pxf);
        }
    }

    @Override
    protected void beforeMethod() throws Exception {
        if (!FDWUtils.useFDW) {
            cleanLogs();
            // Table to check the sessions in the pg_stat_activity table
            pgStatActivity = new Table("pgStatActivityResult", null);
        }
    }

    @Override
    protected void afterMethod() throws Exception {
        // Need to restart pxf service to clean the sessions and shutdown pools
        if (!FDWUtils.useFDW) {
            cluster.restart(PhdCluster.EnumClusterServices.pxf);
        }
    }

    @Test(groups = {"arenadata"})
    public void reloadAllDuringRead() throws Exception {
        createReadableExternalTable(EXT_READ_TABLE_NAME, SOURCE_TABLE_NAME, PXF_RELOAD_SERVER_PROFILE);
        createReadableExternalTable(EXT_READ_TABLE_NAME_2, SOURCE_TABLE_NAME_2, PXF_RELOAD_SECOND_SERVER_PROFILE);

        executeQueryAsCommand(
                String.format(PSQL_SELECT_PG_TEMPLATE, EXT_READ_TABLE_NAME, EXT_READ_TABLE_NAME),
                String.format(SELECT_INT_QUERY_PART_TEMPLATE, SOURCE_TABLE_NAME)
        );
        executeQueryAsCommand(
                String.format(PSQL_SELECT_PG_TEMPLATE, EXT_READ_TABLE_NAME_2, EXT_READ_TABLE_NAME_2),
                String.format(SELECT_INT_QUERY_PART_TEMPLATE, SOURCE_TABLE_NAME_2)
        );

        checkSessionCount(SELECT_EXT_QUERY_PART, 2);

        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a");
        int shutDownPoolCount = 0;
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, PXF_TEMP_LOG_PATH);
            copyLogs(getMethodName(), pxfNode.getHost());
            // Must be 1 record on each segment host
            String result = getCmdResult(cluster, String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_FILE, "profile=, server="));
            assertEquals("1", result);
            // Must be 2 records for all segments because there are only 2 queries.
            shutDownPoolCount += Integer.parseInt(getCmdResult(cluster, String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_FILE, "Shutdown completed.")));
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_FILE, false);
        }
        assertEquals(2, shutDownPoolCount);
        checkSessionCount(SELECT_EXT_QUERY_PART, 0);
    }

    @Test(groups = {"arenadata"})
    public void reloadAllServersForProfileDuringRead() throws Exception {
        createReadableExternalTable(EXT_READ_TABLE_NAME, SOURCE_TABLE_NAME, PXF_RELOAD_SERVER_PROFILE);
        createReadableExternalTable(EXT_READ_TABLE_NAME_2, SOURCE_TABLE_NAME_2, PXF_RELOAD_SECOND_SERVER_PROFILE);

        executeQueryAsCommand(
                String.format(PSQL_SELECT_PG_TEMPLATE, EXT_READ_TABLE_NAME, EXT_READ_TABLE_NAME),
                String.format(SELECT_INT_QUERY_PART_TEMPLATE, SOURCE_TABLE_NAME)
        );
        executeQueryAsCommand(
                String.format(PSQL_SELECT_PG_TEMPLATE, EXT_READ_TABLE_NAME_2, EXT_READ_TABLE_NAME_2),
                String.format(SELECT_INT_QUERY_PART_TEMPLATE, SOURCE_TABLE_NAME_2)
        );

        checkSessionCount(SELECT_EXT_QUERY_PART, 2);

        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p jdbc");

        int shutDownPoolCount = 0;
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, PXF_TEMP_LOG_PATH);
            copyLogs(getMethodName(), pxfNode.getHost());
            // Must be 1 record on each segment host
            String result = getCmdResult(cluster, String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_FILE, "profile=jdbc, server="));
            assertEquals("1", result);
            // Must be 2 records for all segments because there are only 2 queries.
            shutDownPoolCount += Integer.parseInt(getCmdResult(cluster, String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_FILE, "Shutdown completed.")));
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_FILE, false);
        }
        assertEquals(2, shutDownPoolCount);
        checkSessionCount(SELECT_EXT_QUERY_PART, 0);
    }

    @Test(groups = {"arenadata"})
    public void reloadJdbcProfileDuringHdfsRead() throws Exception {
        prepareHdfsAndExtTable();
        createReadableExternalTable(EXT_READ_TABLE_NAME, SOURCE_TABLE_NAME, PXF_RELOAD_DEFAULT_PROFILE);
        createReadableExternalTable(EXT_READ_TABLE_NAME_2, SOURCE_TABLE_NAME_2, PXF_RELOAD_DEFAULT_PROFILE);

        executeQueryAsCommand(
                String.format(PSQL_SELECT_PG_TEMPLATE, EXT_READ_TABLE_NAME, EXT_READ_TABLE_NAME),
                String.format(SELECT_INT_QUERY_PART_TEMPLATE, SOURCE_TABLE_NAME)
        );
        executeQueryAsCommand(
                String.format(PSQL_SELECT_PG_TEMPLATE, EXT_READ_TABLE_NAME_2, EXT_READ_TABLE_NAME_2),
                String.format(SELECT_INT_QUERY_PART_TEMPLATE, SOURCE_TABLE_NAME_2)
        );
        cluster.runCommand(String.format(PSQL_SELECT_HDFS_TEMPLATE, exTable.getName(), exTable.getName()));

        checkSessionCount(SELECT_EXT_QUERY_PART, 2);

        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p jdbc -s default");

        int shutDownPoolCount = 0;
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, PXF_TEMP_LOG_PATH);
            copyLogs(getMethodName(), pxfNode.getHost());
            // Must be 1 record on each segment host
            String result = getCmdResult(cluster, String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_FILE, "profile=jdbc, server=default"));
            assertEquals("1", result);
            // Maybe 1 or 2 records for all segments. It depends on where 2 queries run. If on the different servers then 2 records.
            // If on the same servers - 1 record.
            shutDownPoolCount += Integer.parseInt(getCmdResult(cluster, String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_FILE, "Shutdown completed.")));
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_FILE, false);
        }
        assertTrue(shutDownPoolCount > 0 && shutDownPoolCount <= 2);
        checkSessionCount(SELECT_EXT_QUERY_PART, 0);
        checkSessionCount(SELECT_EXT_QUERY_HDFS_PART, 1);
    }

    @Test(groups = {"arenadata"})
    public void reloadOneServerProfileDuringRead() throws Exception {
        createReadableExternalTable(EXT_READ_TABLE_NAME, SOURCE_TABLE_NAME, PXF_RELOAD_SERVER_PROFILE);
        createReadableExternalTable(EXT_READ_TABLE_NAME_2, SOURCE_TABLE_NAME_2, PXF_RELOAD_SECOND_SERVER_PROFILE);

        executeQueryAsCommand(
                String.format(PSQL_SELECT_PG_TEMPLATE, EXT_READ_TABLE_NAME, EXT_READ_TABLE_NAME),
                String.format(SELECT_INT_QUERY_PART_TEMPLATE, SOURCE_TABLE_NAME)
        );
        executeQueryAsCommand(
                String.format(PSQL_SELECT_PG_TEMPLATE, EXT_READ_TABLE_NAME_2, EXT_READ_TABLE_NAME_2),
                String.format(SELECT_INT_QUERY_PART_TEMPLATE, SOURCE_TABLE_NAME_2)
        );

        checkSessionCount(SELECT_EXT_QUERY_PART, 2);

        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p jdbc -s " + PXF_RELOAD_SERVER_PROFILE);

        int shutDownPoolCount = 0;
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, PXF_TEMP_LOG_PATH);
            copyLogs(getMethodName(), pxfNode.getHost());
            // Must be 1 record on each segment host
            String result = getCmdResult(cluster, String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_FILE, "profile=jdbc, server=reload"));
            assertEquals("1", result);
            // Must be 1 record for all segments because we reload only 1 profile
            shutDownPoolCount += Integer.parseInt(getCmdResult(cluster, String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_FILE, "Shutdown completed.")));
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_FILE, false);
        }
        assertEquals(1, shutDownPoolCount);
        checkSessionCount(SELECT_EXT_QUERY_PART, 1);
    }

    @Test(groups = {"arenadata"})
    public void reloadAllDuringWrite() throws Exception {
        prepareWriteTables(EXT_WRITE_TABLE_NAME, TARGET_TABLE_NAME, PXF_RELOAD_SERVER_PROFILE);
        prepareWriteTables(EXT_WRITE_TABLE_NAME_2, TARGET_TABLE_NAME_2, PXF_RELOAD_SECOND_SERVER_PROFILE);

        executeQueryAsCommand(
                String.format(PSQL_INSERT_PG_TEMPLATE, EXT_WRITE_TABLE_NAME),
                String.format(INSERT_INT_QUERY_PART_TEMPLATE, TARGET_TABLE_NAME)
        );
        executeQueryAsCommand(
                String.format(PSQL_INSERT_PG_TEMPLATE, EXT_WRITE_TABLE_NAME_2),
                String.format(INSERT_INT_QUERY_PART_TEMPLATE, TARGET_TABLE_NAME_2)
        );

        checkSessionCount(INSERT_EXT_QUERY_PART, 2);

        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a");
        int shutDownPoolCount = 0;
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, PXF_TEMP_LOG_PATH);
            copyLogs(getMethodName(), pxfNode.getHost());
            // Must be 1 record on each segment host
            String result = getCmdResult(cluster, String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_FILE, "profile=, server="));
            assertEquals("1", result);
            // Must be 2 record per each segment host because jdbc writable table run query for each logical segment
            // Each segment host starts 1 pool for each query with different profiles
            shutDownPoolCount += Integer.parseInt(getCmdResult(cluster, String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_FILE, "Shutdown completed.")));
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_FILE, false);
        }
        assertEquals(4, shutDownPoolCount);
        checkSessionCount(INSERT_EXT_QUERY_PART, 0);
    }

    @Test(groups = {"arenadata"})
    public void reloadOneServerProfileDuringWrite() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);

        prepareWriteTables(EXT_WRITE_TABLE_NAME, TARGET_TABLE_NAME, PXF_RELOAD_SERVER_PROFILE);
        prepareWriteTables(EXT_WRITE_TABLE_NAME_2, TARGET_TABLE_NAME_2, PXF_RELOAD_SECOND_SERVER_PROFILE);

        executeQueryAsCommand(
                String.format(PSQL_INSERT_PG_TEMPLATE, EXT_WRITE_TABLE_NAME),
                String.format(INSERT_INT_QUERY_PART_TEMPLATE, TARGET_TABLE_NAME)
        );
        executeQueryAsCommand(
                String.format(PSQL_INSERT_PG_TEMPLATE, EXT_WRITE_TABLE_NAME_2),
                String.format(INSERT_INT_QUERY_PART_TEMPLATE, TARGET_TABLE_NAME_2)
        );

        checkSessionCount(INSERT_EXT_QUERY_PART, 2);

        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p jdbc -s " + PXF_RELOAD_SERVER_PROFILE);

        int shutDownPoolCount = 0;
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, PXF_TEMP_LOG_PATH);
            copyLogs(getMethodName(), pxfNode.getHost());
            // Must be 1 record on each segment host
            String result = getCmdResult(cluster, String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_FILE, "profile=jdbc, server=reload"));
            assertEquals("1", result);
            // Must be 1 record per each segment host because jdbc writable table run query for each logical segment
            shutDownPoolCount += Integer.parseInt(getCmdResult(cluster, String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_FILE, "Shutdown completed.")));
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_FILE, false);
        }
        assertEquals(2, shutDownPoolCount);
        checkSessionCount(INSERT_EXT_QUERY_PART, 1);
    }

    @Step("Prepare source table")
    private void prepareSourceTable(String tableName) throws Exception {
        Table gpdbSourceTable = new Table(tableName, TABLE_FIELDS);
        gpdbSourceTable.setDistributionFields(new String[]{"name"});
        gpdb.createTableAndVerify(gpdbSourceTable);
        String queryTemplate = StringUtils.chop(PSQL_INSERT_PG_TEMPLATE);
        cluster.runCommand(String.format(queryTemplate, tableName));
    }

    @Step("Execute query as a command")
    private void executeQueryAsCommand(String command, String internalQuery) throws ShellCommandErrorException, IOException {
        cluster.runCommand(command);
        waitForQueryRun(internalQuery);
    }

    @Step("Check session count")
    private void checkSessionCount(String text, int expectedCount) throws Exception {
        Table gpStatActivityResult = TableFactory.getPxfJdbcReadableTable("gpStatActivityResult",
                null, null, null);
        gpdb.queryResults(gpStatActivityResult, PG_STAT_ACTIVITY_QUERY);
        System.out.println(gpStatActivityResult.getData());
        Assert.assertEquals(countArrayListsWithField(gpStatActivityResult.getData(), text), expectedCount, String.format("Should be %s sessions with query", expectedCount));
    }

    @Step("Prepare write source tables")
    private void prepareWriteTables(String tableName, String dataSourcePath, String serverProfile) throws Exception {
        Table gpdbTargetTable = new Table(dataSourcePath, TABLE_FIELDS);
        gpdbTargetTable.setDistributionFields(new String[]{"name"});
        gpdb.createTableAndVerify(gpdbTargetTable);
        createWritableExternalTable(tableName, dataSourcePath, serverProfile);
    }

    @Step("Create readable external table")
    private void createReadableExternalTable(String tableName, String dataSourcePath, String serverProfile) throws Exception {
        ExternalTable pxfJdbcNamedQuery = TableFactory.getPxfJdbcReadableTable(
                tableName,
                TABLE_FIELDS,
                dataSourcePath,
                serverProfile);
        pxfJdbcNamedQuery.setHost(pxfHost);
        pxfJdbcNamedQuery.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcNamedQuery);
    }

    @Step("Create writable external table")
    private void createWritableExternalTable(String tableName, String dataSourcePath, String serverProfile) throws Exception {
        ExternalTable pxfJdbcNamedQuery = TableFactory.getPxfJdbcWritableTable(
                tableName,
                TABLE_FIELDS,
                dataSourcePath,
                serverProfile);
        pxfJdbcNamedQuery.setHost(pxfHost);
        pxfJdbcNamedQuery.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcNamedQuery);
    }

    @Step("Prepare HDFS and external tables")
    private void prepareHdfsAndExtTable() throws Exception {
        super.beforeMethod();
        hdfsFilePath = hdfs.getWorkingDirectory() + "/data";
        dataTable = new Table("dataTable", null);
        FileFormatsUtils.prepareData(new CustomTextPreparer(), 1000000, dataTable);
        exTable = TableFactory.getPxfReadableTextTable("pxf_hdfs_small_data",
                new String[]{
                        "s1 text",
                        "s2 text",
                        "s3 text",
                        "d1 timestamp",
                        "n1 int",
                        "n2 int",
                        "n3 int",
                        "n4 int",
                        "n5 int",
                        "n6 int",
                        "n7 int",
                        "s11 text",
                        "s12 text",
                        "s13 text",
                        "d11 timestamp",
                        "n11 int",
                        "n12 int",
                        "n13 int",
                        "n14 int",
                        "n15 int",
                        "n16 int",
                        "n17 int"},
                protocol.getExternalTablePath(hdfs.getBasePath(), hdfsFilePath),
                ",");

        exTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.LineBreakAccessor");
        exTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        exTable.setProfile("test:text");
        gpdb.createTableAndVerify(exTable);
        hdfs.writeTableToFile(hdfsFilePath, dataTable, ",");
    }

    @Step("Clean pxf logs")
    private void cleanLogs() throws Exception {
        cluster.runCommandOnNodes(pxfNodes, "> " + pxfLogFile);
    }

    @Step("Copy pxf logs")
    private void copyLogs(String methodName, String host) throws Exception {
        cluster.runCommand("cp " + PXF_TEMP_LOG_FILE + " " + PXF_TEMP_LOG_FILE + "-" + methodName + "-" + host);
    }

    @Step("Wait for query run")
    private void waitForQueryRun(String queryPart) {
        String query = PG_STAT_ACTIVITY_COUNT_QUERY + " AND query ilike '%" + queryPart + "%'";
        Awaitility.waitAtMost(Duration.of(INIT_QUERY_DELAY, ChronoUnit.SECONDS))
                .pollInterval(INIT_QUERY_POLL_INTERVAL, SECONDS)
                .untilAsserted(() -> assertTrue(isQueryRunning(query)));
    }

    private boolean isQueryRunning(String query) throws Exception {
        gpdb.queryResults(pgStatActivity, query);
        String result = pgStatActivity.getData().get(0).get(0);
        return Integer.parseInt(result) > 0;
    }

    private String getMethodName() throws Exception {
        return Thread.currentThread()
                .getStackTrace()[2]
                .getMethodName();
    }

    private int countArrayListsWithField(List<List<String>> listOfLists, String searchText) {
        int count = 0;
        for (List<String> innerList : listOfLists) {
            for (String field : innerList) {
                if (field != null && field.contains(searchText)) {
                    count++;
                    break;
                }
            }
        }
        return count;
    }
}