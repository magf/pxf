package org.greenplum.pxf.automation.arenadata;

import org.greenplum.pxf.automation.components.cluster.MultiNodeCluster;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.CoordinatorNode;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.SegmentNode;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.greenplum.pxf.automation.PxfTestConstant.PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE;

public class PxfReloadTest extends BaseFeature {
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
    }

    @Test(groups = {"arenadata"})
    public void reloadAllDuringRead() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String extTable1 = prepareReadTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareReadTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        cluster.runCommand(String.format("psql -d pxfautomation -c \"select md5(t1.name) from %s t1 join %s t2 on t1.name = t2.name;\" &", extTable1, extTable1));
        cluster.runCommand(String.format("psql -d pxfautomation -c \"select md5(t1.name) from %s t1 join %s t2 on t1.name = t2.name;\" &", extTable2, extTable2));

        long sessionCountBeforeReload = getGpdbSessionCountWithText("select md5(t1.name) from");
        Assert.assertEquals(sessionCountBeforeReload, 2, "Should be two sessions with select");
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a");
        long sessionCountAfterReload = getGpdbSessionCountWithText("select md5(t1.name) from");
        Assert.assertEquals(sessionCountAfterReload, 0, "Should be zero session with select");

        Assert.assertEquals(sessionCountBeforeReload - sessionCountAfterReload, 6, "Two sessions should be closed");
        checkStringInPxfLog("profile=, server=", 1);
        checkStringInPxfLog("Shutdown completed.", 2);
    }

    @Test(groups = {"arenadata"})
    public void reloadAllServersForProfileDuringRead() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String extTable1 = prepareReadTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareReadTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        cluster.runCommand(String.format("psql -d pxfautomation -c \"select md5(t1.name) from %s t1 join %s t2 on t1.name = t2.name;\" &", extTable1, extTable1));
        cluster.runCommand(String.format("psql -d pxfautomation -c \"select md5(t1.name) from %s t1 join %s t2 on t1.name = t2.name;\" &", extTable2, extTable2));

        long sessionCountBeforeReload = getGpdbSessionCountWithText("select md5(t1.name) from");
        Assert.assertEquals(sessionCountBeforeReload, 2, "Should be two sessions with select");
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p jdbc");
        long sessionCountAfterReload = getGpdbSessionCountWithText("select md5(t1.name) from");
        Assert.assertEquals(sessionCountAfterReload, 0, "Should be zero session with select");

        Assert.assertEquals(sessionCountBeforeReload - sessionCountAfterReload, 6, "Two sessions should be closed");
        checkStringInPxfLog("profile=, server=", 1);
        checkStringInPxfLog("Shutdown completed.", 2);
    }

    @Test(groups = {"arenadata"})
    public void reloadJdbcProfileForDefaultServerDuringRead() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String extTable1 = prepareReadTables("table", "default");
        String extTable2 = prepareReadTables("table2", "default");

        cluster.runCommand(String.format("psql -d pxfautomation -c \"select md5(t1.name) from %s t1 join %s t2 on t1.name = t2.name;\" &", extTable1, extTable1));
        cluster.runCommand(String.format("psql -d pxfautomation -c \"select md5(t1.name) from %s t1 join %s t2 on t1.name = t2.name;\" &", extTable2, extTable2));

        long sessionCountBeforeReload = getGpdbSessionCountWithText("select md5(t1.name) from");
        Assert.assertEquals(sessionCountBeforeReload, 2, "Should be two sessions with select");
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p hdfs");
        long sessionCountAfterReload = getGpdbSessionCountWithText("select md5(t1.name) from");
        Assert.assertEquals(sessionCountAfterReload, 0, "Should be zero session with select");

        Assert.assertEquals(sessionCountBeforeReload - sessionCountAfterReload, 6, "Two sessions should be closed");
        checkStringInPxfLog("profile=, server=", 1);
        checkStringInPxfLog("Shutdown completed.", 2);
    }

    @Test(groups = {"arenadata"})
    public void reloadOneServerProfileDuringRead() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String extTable1 = prepareReadTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareReadTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        cluster.runCommand(String.format("psql -d pxfautomation -c \"select md5(t1.name) from %s t1 join %s t2 on t1.name = t2.name;\" &", extTable1, extTable1));
        cluster.runCommand(String.format("psql -d pxfautomation -c \"select md5(t1.name) from %s t1 join %s t2 on t1.name = t2.name;\" &", extTable2, extTable2));

        long sessionCountBeforeReload = getGpdbSessionCountWithText("select md5(t1.name) from");
        Assert.assertEquals(sessionCountBeforeReload, 2, "Should be two sessions with select");
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p jdbc -s " + PXF_RELOAD_SERVER_PROFILE);
        long sessionCountAfterReload = getGpdbSessionCountWithText("select md5(t1.name) from");
        Assert.assertEquals(sessionCountAfterReload, 1, "Should be one session with select");

        Assert.assertEquals(sessionCountBeforeReload - sessionCountAfterReload, 3, "One sessions should be closed");
        checkStringInPxfLog("profile=jdbc, server=reload", 1);
        checkStringInPxfLog("Shutdown completed.", 1);
    }

    @Test(groups = {"arenadata"})
    public void reloadAllDuringWrite() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String extTable1 = prepareWriteTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareWriteTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        cluster.runCommand(String.format("psql -d pxfautomation -c \"INSERT INTO %s SELECT i, md5(random()::text) from generate_series(1,8000000) i;\" &", extTable1));
        cluster.runCommand(String.format("psql -d pxfautomation -c \"INSERT INTO %s SELECT i, md5(random()::text) from generate_series(1,8000000) i;\" &", extTable2));

        long sessionCountBeforeReload = getGpdbSessionCountWithText("INSERT INTO write_ext_table");
        Assert.assertEquals(sessionCountBeforeReload, 2, "Should be two sessions with insert");
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a");
        long sessionCountAfterReload = getGpdbSessionCountWithText("INSERT INTO write_ext_table");
        Assert.assertEquals(sessionCountAfterReload, 0, "Should be zero sessions with insert");

        Assert.assertEquals(sessionCountBeforeReload - sessionCountAfterReload, 6, "Two sessions should be closed");
        checkStringInPxfLog("profile=, server=", 1);
        checkStringInPxfLog("Shutdown completed.", 2);
    }

    @Test(groups = {"arenadata"})
    public void reloadOneServerProfileDuringWrite() throws Exception {
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String extTable1 = prepareWriteTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareWriteTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        cluster.runCommand(String.format("psql -d pxfautomation -c \"INSERT INTO %s SELECT i, md5(random()::text) from generate_series(1,8000000) i;\" &", extTable1));
        cluster.runCommand(String.format("psql -d pxfautomation -c \"INSERT INTO %s SELECT i, md5(random()::text) from generate_series(1,8000000) i;\" &", extTable2));

        long sessionCountBeforeReload = getGpdbSessionCountWithText("INSERT INTO write_ext_table");
        Assert.assertEquals(sessionCountBeforeReload, 2, "Should be two sessions with insert");
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
        cluster.runCommandOnNodes(Collections.singletonList(masterNode), "pxf cluster reload -a -p jdbc -s " + PXF_RELOAD_SERVER_PROFILE);
        long sessionCountAfterReload = getGpdbSessionCountWithText("INSERT INTO write_ext_table");
        Assert.assertEquals(sessionCountAfterReload, 1, "Should be one sessions with insert");

        checkStringInPxfLog("profile=jdbc, server=reload", 1);
        checkStringInPxfLog("Shutdown completed.", 1);
    }

    private void checkStringInPxfLog(String logLine, int countInLogs) throws Exception {
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode),
                String.format("cat %s | grep \"%s\" | { [ $(wc -l) -eq %d ] && exit 0 || exit 1; }", pxfLogFile, logLine, countInLogs));
    }

    private long getGpdbSessionCountWithText(String text) throws Exception {
        Table gpStatActivityResult = TableFactory.getPxfJdbcReadableTable("gpStatActivityResult",
                null, null, null);
        gpdb.queryResults(gpStatActivityResult, "select * from pg_stat_activity where usename = 'gpadmin';");
        return gpStatActivityResult.getData().stream().filter(row -> row.stream().filter(field -> field.contains(text)).count() > 0).count();
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
}