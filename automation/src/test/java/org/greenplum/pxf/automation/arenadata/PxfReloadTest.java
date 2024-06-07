package org.greenplum.pxf.automation.arenadata;

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
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.greenplum.pxf.automation.PxfTestConstant.PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PxfReloadTest extends BaseFeature {
    private static final String SUFFIX_CLASS = ".class";
    private static final String INSERT_QUERY_PART = "INSERT INTO write_ext_table";
    private static final String SELECT_QUERY_PG_PART = "select md5(t1.name) from";
    private static final String SELECT_QUERY_HDFS_PART = "select md5(t1.s1) from";
    private static final String PSQL_SELECT_PG_TEMPLATE = "psql -d pxfautomation -c \"select md5(t1.name) from %s t1 join %s t2 on t1.name = t2.name;\" &";
    private static final String PSQL_INSERT_PG_TEMPLATE = "psql -d pxfautomation -c \"INSERT INTO %s SELECT i, md5(random()::text) from generate_series(1,8000000) i;\" &";
    public static final String PSQL_SELECT_HDFS_TEMPLATE = "psql -d pxfautomation -c \"select md5(t1.s1) from %s t1 join %s t2 on t1.s1 = t2.s1;\" &";
    ProtocolEnum protocol;
    Table dataTable = null;
    String hdfsFilePath = "";
    String testPackageLocation = "/org/greenplum/pxf/automation/testplugin/";
    String throwOn10000Accessor = "ThrowOn10000Accessor";
    private List<Node> pxfNodes;
    private Node masterNode;
    private String pxfLogFile;
    private static final String PXF_LOG_RELATIVE_PATH = "logs/pxf-service.log";
    private static final String PXF_RELOAD_SERVER_PROFILE = "reload";
    private static final String PXF_RELOAD_SECOND_SERVER_PROFILE = "reload-second";
    private static final String PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH = "templates/pxf-reload/jdbc-site.xml";
    private static final String PXF_TEMP_LOG_PATH = "/tmp/pxf-service.log";
    private static final String GREP_COMMAND_TEMPLATE = "cat %s | grep \"%s\" | wc -l";
    private static final String[] TABLE_FIELDS = new String[]{
            "id  int",
            "name text"};

    @Override
    protected void beforeClass() throws Exception {
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
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        protocol = ProtocolUtils.getProtocol();
    }

    @Test(groups = {"arenadata"})
    public void reloadAllDuringRead() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String extTable1 = prepareReadTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareReadTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        cluster.runCommand(String.format(PSQL_SELECT_PG_TEMPLATE, extTable1, extTable1));
        cluster.runCommand(String.format(PSQL_SELECT_PG_TEMPLATE, extTable2, extTable2));
        checkSessionCount(SELECT_QUERY_PG_PART, 2);
        cluster.runCommandOnNodes(pxfNodes, "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a");
        int shutDownPoolCount = 0;
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, "/tmp/");
            // Must be 1 record on each segment host
            String result = grepLog(String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_PATH, "profile=, server="));
            assertEquals("1", result);
            // Must be 2 records for all segments because there are only 2 queries.
            shutDownPoolCount += Integer.parseInt(grepLog(String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_PATH, "Shutdown completed.")));
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_PATH, false);
        }
        assertEquals(2, shutDownPoolCount);
        checkSessionCount(SELECT_QUERY_PG_PART, 0);
    }

    @Test(groups = {"arenadata"})
    public void reloadAllServersForProfileDuringRead() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String extTable1 = prepareReadTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareReadTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        cluster.runCommand(String.format(PSQL_SELECT_PG_TEMPLATE, extTable1, extTable1));
        cluster.runCommand(String.format(PSQL_SELECT_PG_TEMPLATE, extTable2, extTable2));

        checkSessionCount(SELECT_QUERY_PG_PART, 2);
        cluster.runCommandOnNodes(pxfNodes, "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p jdbc");

        int shutDownPoolCount = 0;
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, "/tmp/");
            // Must be 1 record on each segment host
            String result = grepLog(String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_PATH, "profile=jdbc, server="));
            assertEquals("1", result);
            // Must be 2 records for all segments because there are only 2 queries.
            shutDownPoolCount += Integer.parseInt(grepLog(String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_PATH, "Shutdown completed.")));
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_PATH, false);
        }
        assertEquals(2, shutDownPoolCount);
        checkSessionCount(SELECT_QUERY_PG_PART, 0);
    }

    @Test(groups = {"arenadata"})
    public void reloadJdbcProfileDuringHdfsRead() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        prepareHdfsAndExtTable();
        String extTable1 = prepareReadTables("table", "default");
        String extTable2 = prepareReadTables("table2", "default");

        cluster.runCommand(String.format(PSQL_SELECT_PG_TEMPLATE, extTable1, extTable1));
        cluster.runCommand(String.format(PSQL_SELECT_PG_TEMPLATE, extTable2, extTable2));
        cluster.runCommand(String.format(PSQL_SELECT_HDFS_TEMPLATE, exTable.getName(), exTable.getName()));

        checkSessionCount(SELECT_QUERY_PG_PART, 2);
        cluster.runCommandOnNodes(pxfNodes, "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p jdbc -s default");

        int shutDownPoolCount = 0;
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, "/tmp/");
            // Must be 1 record on each segment host
            String result = grepLog(String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_PATH, "profile=jdbc, server=default"));
            assertEquals("1", result);
            // Maybe 1 or 2 records for all segments. It depends on where 2 queries run. If on the different servers then 2 records.
            // If on the same servers - 1 record.
            shutDownPoolCount += Integer.parseInt(grepLog(String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_PATH, "Shutdown completed.")));
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_PATH, false);
        }
        assertTrue(shutDownPoolCount > 0 && shutDownPoolCount <= 2);
        checkSessionCount(SELECT_QUERY_PG_PART, 0);
        checkSessionCount(SELECT_QUERY_HDFS_PART, 1);
    }

    @Test(groups = {"arenadata"})
    public void reloadOneServerProfileDuringRead() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String extTable1 = prepareReadTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareReadTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        cluster.runCommand(String.format(PSQL_SELECT_PG_TEMPLATE, extTable1, extTable1));
        cluster.runCommand(String.format(PSQL_SELECT_PG_TEMPLATE, extTable2, extTable2));

        checkSessionCount(SELECT_QUERY_PG_PART, 2);
        cluster.runCommandOnNodes(pxfNodes, "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p jdbc -s " + PXF_RELOAD_SERVER_PROFILE);

        int shutDownPoolCount = 0;
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, "/tmp/");
            // Must be 1 record on each segment host
            String result = grepLog(String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_PATH, "profile=jdbc, server=reload"));
            assertEquals("1", result);
            // Must be 1 record for all segments because we reload only 1 profile
            shutDownPoolCount += Integer.parseInt(grepLog(String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_PATH, "Shutdown completed.")));
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_PATH, false);
        }
        assertEquals(1, shutDownPoolCount);
        checkSessionCount(SELECT_QUERY_PG_PART, 1);
    }

    @Test(groups = {"arenadata"})
    public void reloadAllDuringWrite() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String extTable1 = prepareWriteTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareWriteTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        cluster.runCommand(String.format(PSQL_INSERT_PG_TEMPLATE, extTable1));
        cluster.runCommand(String.format(PSQL_INSERT_PG_TEMPLATE, extTable2));

        checkSessionCount(INSERT_QUERY_PART, 2);
        cluster.runCommandOnNodes(pxfNodes, "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a");
        int shutDownPoolCount = 0;
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, "/tmp/");
            // Must be 1 record on each segment host
            String result = grepLog(String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_PATH, "profile=, server="));
            assertEquals("1", result);
            // Must be 1 record per each segment host because jdbc writable table run query for each logical segment
            shutDownPoolCount += Integer.parseInt(grepLog(String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_PATH, "Shutdown completed.")));
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_PATH, false);
        }
        assertEquals(2, shutDownPoolCount);
        checkSessionCount(INSERT_QUERY_PART, 0);
    }

    @Test(groups = {"arenadata"})
    public void reloadOneServerProfileDuringWrite() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String extTable1 = prepareWriteTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareWriteTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        cluster.runCommand(String.format(PSQL_INSERT_PG_TEMPLATE, extTable1));
        cluster.runCommand(String.format(PSQL_INSERT_PG_TEMPLATE, extTable2));

        checkSessionCount(INSERT_QUERY_PART, 2);
        cluster.runCommandOnNodes(pxfNodes, "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p jdbc -s " + PXF_RELOAD_SERVER_PROFILE);

        int shutDownPoolCount = 0;
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, "/tmp/");
            // Must be 1 record on each segment host
            String result = grepLog(String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_PATH, "profile=jdbc, server=reload"));
            assertEquals("1", result);
            // Must be 1 record per each segment host because jdbc writable table run query for each logical segment
            shutDownPoolCount += Integer.parseInt(grepLog(String.format(GREP_COMMAND_TEMPLATE, PXF_TEMP_LOG_PATH, "Shutdown completed.")));
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_PATH, false);
        }
        assertEquals(2, shutDownPoolCount);
        checkSessionCount(INSERT_QUERY_PART, 1);
    }

    private void checkSessionCount(String text, int expectedCount) throws Exception {
        Table gpStatActivityResult = TableFactory.getPxfJdbcReadableTable("gpStatActivityResult",
                null, null, null);
        gpdb.queryResults(gpStatActivityResult, "select * from pg_stat_activity where usename = 'gpadmin';");
        System.out.println(gpStatActivityResult.getData());
        Assert.assertEquals(countArrayListsWithField(gpStatActivityResult.getData(), text), expectedCount, String.format("Should be %s sessions with query", expectedCount));
    }

    private String prepareReadTables(String tableName, String serverProfile) throws Exception {
        String extTableName = "read_ext_" + tableName;
        String sourceTableName = "gpdb_source_" + tableName;
        Table gpdbSourceTable = new Table(sourceTableName, TABLE_FIELDS);
        gpdbSourceTable.setDistributionFields(new String[]{"name"});
        gpdb.createTableAndVerify(gpdbSourceTable);
        gpdb.runQuery("INSERT INTO " + sourceTableName + " SELECT i, md5(random()::text) from generate_series(1,8000000) i;");
        createReadableExternalTable(extTableName, "public." + sourceTableName, serverProfile);
        return extTableName;
    }

    private String prepareWriteTables(String tableName, String serverProfile) throws Exception {
        String extTableName = "write_ext_" + tableName;
        String sourceTableName = "gpdb_source_" + tableName;
        Table gpdbSourceTable = new Table(sourceTableName, TABLE_FIELDS);
        gpdbSourceTable.setDistributionFields(new String[]{"name"});
        gpdb.createTableAndVerify(gpdbSourceTable);
        createWritableExternalTable(extTableName, "public." + sourceTableName, serverProfile);
        return extTableName;
    }

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

    private String grepLog(String command) throws ShellCommandErrorException, IOException {
        cluster.runCommand(command);
        String result = cluster.getLastCmdResult();
        String[] results = result.split("\r\n");
        return results.length > 1 ? results[1].trim() : "Result is empty";
    }
}