package org.greenplum.pxf.automation.arenadata;

import org.greenplum.pxf.automation.components.cluster.MultiNodeCluster;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.SegmentNode;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.greenplum.pxf.automation.PxfTestConstant.PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE;

public class PxfReloadTest extends BaseFeature {
    private Node pxfNode;
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
        }
        pxfLogFile = pxfHome + "/" + PXF_LOG_RELATIVE_PATH;
    }

    @Test(groups = {"arenadata"})
    public void reloadAll() throws Exception {
        String extTable1 = prepareTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        gpdb.runQuery("select * from " + extTable1, true, true);
        gpdb.runQuery("select * from " + extTable2, true, true);

        int sessionCountBeforeReload = getGpdbSessionCount();
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
        cluster.runCommand("pxf cluster reload -a", 0);
        int sessionCountAfterReload = getGpdbSessionCount();

        Assert.assertEquals(sessionCountBeforeReload - sessionCountAfterReload, 2, "Two sessions should be closed");
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode),
                "cat " + pxfLogFile + " | grep \"profile=, server=\" | { [ $(wc -l) -eq 1 ] && exit 0 || exit 1; }");
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode),
                "cat " + pxfLogFile + " | grep \"Shutdown completed.\" | { [ $(wc -l) -eq 2 ] && exit 0 || exit 1; }");
    }

    @Test(groups = {"arenadata"})
    public void reloadOneServerProfile() throws Exception {
        String extTable1 = prepareTables("table", PXF_RELOAD_SERVER_PROFILE);
        String extTable2 = prepareTables("table2", PXF_RELOAD_SECOND_SERVER_PROFILE);

        gpdb.runQuery("select * from " + extTable1, true, true);
        gpdb.runQuery("select * from " + extTable2, true, true);

        int sessionCountBeforeReload = getGpdbSessionCount();
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode), "> " + pxfLogFile);
        cluster.runCommand("pxf cluster reload -a -p jdbc -s " + PXF_RELOAD_SERVER_PROFILE, 0);
        int sessionCountAfterReload = getGpdbSessionCount();

        Assert.assertEquals(sessionCountBeforeReload - sessionCountAfterReload, 1, "One sessions should be closed");
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode),
                "cat " + pxfLogFile + " | grep \"profile=jdbc, server=reload\" | { [ $(wc -l) -eq 1 ] && exit 0 || exit 1; }");
        cluster.runCommandOnNodes(Collections.singletonList(pxfNode),
                "cat " + pxfLogFile + " | grep \"Shutdown completed.\" | { [ $(wc -l) -eq 1 ] && exit 0 || exit 1; }");
    }

    private int getGpdbSessionCount() throws Exception {
        Table gpStatActivityResult = TableFactory.getPxfJdbcReadableTable("gpStatActivityResult",
                null, null, null);
        gpdb.queryResults(gpStatActivityResult, "select * from pg_stat_activity where usename = 'gpadmin';");
        return gpStatActivityResult.getData().size();
    }

    private String prepareTables(String tableName, String serverProfile) throws Exception {
        String extTableName = "read_ext_" + tableName;
        prepareSourceTable("gpdb_source_" + tableName);
        createExternalTable(extTableName, "public.gpdb_source_" + tableName, serverProfile);
        return extTableName;
    }

    private void createExternalTable(String tableName, String dataSourcePath, String serverProfile) throws Exception {
        ExternalTable pxfJdbcNamedQuery = TableFactory.getPxfJdbcReadableTable(
                tableName,
                TABLE_FIELDS,
                dataSourcePath,
                serverProfile);
        pxfJdbcNamedQuery.setHost(pxfHost);
        pxfJdbcNamedQuery.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcNamedQuery);
    }

    private void prepareSourceTable(String tableName) throws Exception {
        Table gpdbSourceTable = new Table(tableName, TABLE_FIELDS);
        gpdbSourceTable.setDistributionFields(new String[]{"name"});
        gpdb.createTableAndVerify(gpdbSourceTable);
        String[][] rows = new String[][]{
                {"1", "text1"},
                {"2", "text2"},
                {"3", "text3"}};
        Table dataTable = new Table("dataTable", TABLE_FIELDS);
        dataTable.addRows(rows);
        gpdb.insertData(dataTable, gpdbSourceTable);
    }
}