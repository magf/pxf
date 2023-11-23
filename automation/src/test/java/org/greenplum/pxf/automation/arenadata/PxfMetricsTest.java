package org.greenplum.pxf.automation.arenadata;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.greenplum.pxf.automation.components.cluster.MultiNodeCluster;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.SegmentNode;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;

public class PxfMetricsTest extends BaseFeature {
    private static final String CLUSTER_NAME = "TestEnv";
    private static final String[] SOURCE_TABLE_FIELDS = new String[]{"id    int"};
    private Table gpdbMetricsSourceTable, gpdbMetricsReadableTable;
    private URL serviceMetricsUrl, clusterMetricsUrl;

    @Override
    protected void beforeClass() throws Exception {
        String pxfAppPropertiesFile = cluster.getPxfHome() + "/conf/pxf-application.properties";
        cluster.runCommandOnAllNodes("sed -i 's/# server.address=localhost/server.address=0\\.0\\.0\\.0\\ncluster-name=" + CLUSTER_NAME + "/' " + pxfAppPropertiesFile);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        String pxfHostName = getPxfHostName();
        serviceMetricsUrl = new URL("http://" + pxfHostName + ":" + pxfPort + "/service-metrics");
        clusterMetricsUrl = new URL("http://" + pxfHostName + ":" + pxfPort + "/service-metrics/cluster-metrics");
        prepareData();
        runQueryWithExternalTable();
    }

    private String getPxfHostName() {
        String pxfHostName;
        if (cluster instanceof MultiNodeCluster) {
            Node pxfNode = ((MultiNodeCluster) cluster).getNode(SegmentNode.class, PhdCluster.EnumClusterServices.pxf).get(0);
            pxfHostName = pxfNode.getHostName();
            if (StringUtils.isEmpty(pxfNode.getHostName())) {
                pxfHostName = pxfNode.getHost();
            }
        } else {
            pxfHostName = cluster.getHost();
        }
        return pxfHostName;
    }

    protected void prepareData() throws Exception {
        prepareMetricsSourceTable();
        createMetricsReadableTable();
    }

    private void prepareMetricsSourceTable() throws Exception {
        gpdbMetricsSourceTable = new Table("metrics_source_table", SOURCE_TABLE_FIELDS);
        gpdbMetricsSourceTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(gpdbMetricsSourceTable);
        Table dataTable = new Table("dataTable", SOURCE_TABLE_FIELDS);
        String[][] rows = new String[][]{
                {"1"},
                {"2"},
                {"3"},
                {"4"}};
        dataTable.addRows(rows);
        gpdb.insertData(dataTable, gpdbMetricsSourceTable);
    }

    private void createMetricsReadableTable() throws Exception {
        gpdbMetricsReadableTable = TableFactory.getPxfJdbcReadableTable(
                "metrics_read_ext_table",
                SOURCE_TABLE_FIELDS,
                gpdbMetricsSourceTable.getName(),
                "default");
        gpdb.createTableAndVerify(gpdbMetricsReadableTable);
    }

    private void runQueryWithExternalTable() throws Exception {
        gpdb.runQuery("SELECT * FROM " + gpdbMetricsReadableTable.getName(), false, true);
    }

    @Test(groups = {"arenadata"}, description = "Check cluster metrics")
    public void testClusterMetrics() throws Exception {
        String expectedPxfRecordsSent = "4.0";
        String actualPxfRecordsSent = "";
        String json = IOUtils.toString(clusterMetricsUrl, StandardCharsets.UTF_8);
        JSONObject jsonObject = new JSONObject(json);
        assertEquals(CLUSTER_NAME, jsonObject.get("cluster"));
        JSONArray metrics = jsonObject.getJSONArray("metrics");
        for (int i = 0; i < metrics.length(); i++) {
            if (metrics.getJSONObject(i).getString("name").equals("pxf.records.sent")) {
                actualPxfRecordsSent = metrics.getJSONObject(i).getJSONArray("measurements").getJSONObject(0).getString("value");
            }
        }
        assertEquals(actualPxfRecordsSent, expectedPxfRecordsSent);
    }

    @Test(groups = {"arenadata"}, description = "Check service metrics")
    public void testServiceMetrics() throws Exception {
        String expectedPxfRecordsSent = "4.0";
        String actualPxfRecordsSent = "";
        String json = IOUtils.toString(serviceMetricsUrl, StandardCharsets.UTF_8);
        JSONArray metrics = new JSONArray(json);
        for (int i = 0; i < metrics.length(); i++) {
            if (metrics.getJSONObject(i).getString("name").equals("pxf.records.sent")) {
                actualPxfRecordsSent = metrics.getJSONObject(i).getJSONArray("measurements").getJSONObject(0).getString("value");
            }
        }
        assertEquals(actualPxfRecordsSent, expectedPxfRecordsSent);
    }
}
