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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class PxfMetricsTest extends BaseFeature {
    private static final String CLUSTER_NAME = "TestEnv";
    private static final String[] SOURCE_TABLE_FIELDS = new String[]{"id    int"};
    private Table gpdbMetricsSourceTable, gpdbMetricsReadableTable;
    private final Collection<URL> serviceMetricsUrls = new ArrayList<>();
    private final Collection<URL> clusterMetricsUrls = new ArrayList<>();

    @Override
    protected void beforeClass() throws Exception {
        String pxfAppPropertiesFile = cluster.getPxfHome() + "/conf/pxf-application.properties";
        cluster.runCommandOnAllNodes("sed -i 's/# server.address=localhost/server.address=0\\.0\\.0\\.0\\ncluster-name=" + CLUSTER_NAME + "/' " + pxfAppPropertiesFile);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        Collection<String> pxfHostNames = getPxfHostNames();
        for (String pxfHostName : pxfHostNames) {
            serviceMetricsUrls.add(new URL("http://" + pxfHostName + ":" + pxfPort + "/service-metrics"));
            clusterMetricsUrls.add(new URL("http://" + pxfHostName + ":" + pxfPort + "/service-metrics/cluster-metrics"));
        }

        prepareData();
        runQueryWithExternalTable();
    }

    private Collection<String> getPxfHostNames() {
        Collection<String> pxfHostNames = new ArrayList<>();
        if (cluster instanceof MultiNodeCluster) {
            List<Node> pxfNodes = ((MultiNodeCluster) cluster).getNode(SegmentNode.class, PhdCluster.EnumClusterServices.pxf);
            for (Node pxfNode : pxfNodes) {
                String pxfHostName = pxfNode.getHostName();
                if (StringUtils.isEmpty(pxfNode.getHostName())) {
                    pxfHostName = pxfNode.getHost();
                }
                pxfHostNames.add(pxfHostName);
            }
        } else {
            pxfHostNames.add(cluster.getHost());
        }
        return pxfHostNames;
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
        float expectedPxfRecordsSent = 4.0f;
        float actualPxfRecordsSent = 0.0f;
        for (URL clusterMetricsUrl : clusterMetricsUrls) {
            String json = IOUtils.toString(clusterMetricsUrl, StandardCharsets.UTF_8);
            JSONObject jsonObject = new JSONObject(json);
            assertEquals(CLUSTER_NAME, jsonObject.get("cluster"));
            JSONArray metrics = jsonObject.getJSONArray("metrics");
            for (int i = 0; i < metrics.length(); i++) {
                if (metrics.getJSONObject(i).getString("name").equals("pxf.records.sent")) {
                    String actualPxfRecordsSentStr = metrics.getJSONObject(i).getJSONArray("measurements")
                            .getJSONObject(0).getString("value");
                    actualPxfRecordsSent += Float.parseFloat(actualPxfRecordsSentStr);
                }
            }
        }
        assertEquals(Float.compare(actualPxfRecordsSent, expectedPxfRecordsSent), 0,
                "Check cluster metrics. If expected value is not 0 then the actual cluster metric is not the same we are expecting");
    }

    @Test(groups = {"arenadata"}, description = "Check service metrics")
    public void testServiceMetrics() throws Exception {
        float expectedPxfRecordsSent = 4.0f;
        float actualPxfRecordsSent = 0.0f;
        for (URL serviceMetricsUrl : serviceMetricsUrls) {
            String json = IOUtils.toString(serviceMetricsUrl, StandardCharsets.UTF_8);
            JSONArray metrics = new JSONObject(json).getJSONArray("metrics");
            for (int i = 0; i < metrics.length(); i++) {
                if (metrics.getJSONObject(i).getString("name").equals("pxf.records.sent")) {
                    String actualPxfRecordsSentStr = metrics.getJSONObject(i).getJSONArray("measurements")
                            .getJSONObject(0).getString("value");
                    actualPxfRecordsSent += Float.parseFloat(actualPxfRecordsSentStr);
                }
            }
        }
        assertEquals(Float.compare(actualPxfRecordsSent, expectedPxfRecordsSent), 0,
                "Check service metrics. If expected value is not 0 then the actual service metric is not the same we are expecting");
    }
}
