package org.greenplum.pxf.automation.arenadata;

import org.greenplum.pxf.automation.components.cluster.MultiNodeCluster;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.CoordinatorNode;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.SegmentNode;
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
import java.util.Collections;
import java.util.List;

import static org.greenplum.pxf.automation.PxfTestConstant.PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE;

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
    private Node pxfNode;
    private Node masterNode;
    private String pxfLogFile;
    private static final String PXF_LOG_RELATIVE_PATH = "logs/pxf-service.log";
    private static final String PXF_RELOAD_SERVER_PROFILE = "reload";
    private static final String PXF_RELOAD_SECOND_SERVER_PROFILE = "reload-second";
    private static final String PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH = "templates/pxf-reload/jdbc-site.xml";
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
            pxfNode = ((MultiNodeCluster) cluster).getNode(SegmentNode.class, PhdCluster.EnumClusterServices.pxf).get(0);
            masterNode = ((MultiNodeCluster) cluster).getNode(CoordinatorNode.class, PhdCluster.EnumClusterServices.pxf).get(0);
        }
        pxfLogFile = pxfHome + "/" + PXF_LOG_RELATIVE_PATH;
        changeLogLevelToInfo();
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
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a");
        checkSessionCount(SELECT_QUERY_PG_PART, 0);

        checkStringInPxfLog("profile=, server=", 1);
        checkStringInPxfLog("Shutdown completed.", 2);
    }

    @Test(groups = {"arenadata"})
    public void reloadAllServersForProfileDuringRead() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String extTable1 = prepareReadTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareReadTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        cluster.runCommand(String.format(PSQL_SELECT_PG_TEMPLATE, extTable1, extTable1));
        cluster.runCommand(String.format(PSQL_SELECT_PG_TEMPLATE, extTable2, extTable2));

        checkSessionCount(SELECT_QUERY_PG_PART, 2);
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p jdbc");
        checkSessionCount(SELECT_QUERY_PG_PART, 0);

        checkStringInPxfLog("profile=jdbc, server=", 1);
        checkStringInPxfLog("Shutdown completed.", 2);
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
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p jdbc -s default");
        checkSessionCount(SELECT_QUERY_PG_PART, 0);
        checkSessionCount(SELECT_QUERY_HDFS_PART, 1);

        checkStringInPxfLog("profile=jdbc, server=default", 1);
        checkStringInPxfLog("Shutdown completed.", 1);
    }

    @Test(groups = {"arenadata"})
    public void reloadOneServerProfileDuringRead() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String extTable1 = prepareReadTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareReadTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        cluster.runCommand(String.format(PSQL_SELECT_PG_TEMPLATE, extTable1, extTable1));
        cluster.runCommand(String.format(PSQL_SELECT_PG_TEMPLATE, extTable2, extTable2));

        checkSessionCount(SELECT_QUERY_PG_PART, 2);
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p jdbc -s " + PXF_RELOAD_SERVER_PROFILE);
        checkSessionCount(SELECT_QUERY_PG_PART, 1);

        checkStringInPxfLog("profile=jdbc, server=reload", 1);
        checkStringInPxfLog("Shutdown completed.", 1);
    }

    @Test(groups = {"arenadata"})
    public void reloadAllDuringWrite() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String extTable1 = prepareWriteTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareWriteTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        cluster.runCommand(String.format(PSQL_INSERT_PG_TEMPLATE, extTable1));
        cluster.runCommand(String.format(PSQL_INSERT_PG_TEMPLATE, extTable2));

        checkSessionCount(INSERT_QUERY_PART, 2);
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a");
        checkSessionCount(INSERT_QUERY_PART, 0);

        checkStringInPxfLog("profile=, server=", 1);
        checkStringInPxfLog("Shutdown completed.", 2);
    }

    @Test(groups = {"arenadata"})
    public void reloadOneServerProfileDuringWrite() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String extTable1 = prepareWriteTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareWriteTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        cluster.runCommand(String.format(PSQL_INSERT_PG_TEMPLATE, extTable1));
        cluster.runCommand(String.format(PSQL_INSERT_PG_TEMPLATE, extTable2));

        checkSessionCount(INSERT_QUERY_PART, 2);
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p jdbc -s " + PXF_RELOAD_SERVER_PROFILE);
        checkSessionCount(INSERT_QUERY_PART, 1);

        checkStringInPxfLog("profile=jdbc, server=reload", 1);
        checkStringInPxfLog("Shutdown completed.", 1);
    }

    private void checkStringInPxfLog(String logLine, int countInLogs) throws Exception {
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode),
                String.format("cat %s | grep \"%s\" | { [ $(wc -l) -eq %d ] && exit 0 || exit 1; }", pxfLogFile, logLine, countInLogs));
    }

    private void checkSessionCount(String text, int expectedCount) throws Exception {
        Table gpStatActivityResult = TableFactory.getPxfJdbcReadableTable("gpStatActivityResult",
                null, null, null);
        gpdb.queryResults(gpStatActivityResult, "select * from pg_stat_activity where usename = 'gpadmin';");
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

    private void changeLogLevelToInfo() throws Exception {
        String pxfAppPropertiesFile = cluster.getPxfHome() + "/conf/pxf-application.properties";
        cluster.runCommandOnAllNodes("sed -i 's/pxf.log.level=trace/# pxf.log.level=info/' " + pxfAppPropertiesFile);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
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