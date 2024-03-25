package org.greenplum.pxf.automation.arenadata;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import java.io.File;

public class JdbcJsonFeatureTest extends BaseFeature {
    private static final String[] JSON_SOURCE_TABLE_FIELDS = new String[]{
            "id    serial",
            "data_jsonb   jsonb",
            "data_json  json"};

    private static final String JSON_FILE_NAME = "jdbc_json.txt";
    private Table gpdbJsonSourceTable;
    private Table gpdbJsonTargetTable;

    @Override
    protected void beforeClass() throws Exception {
        prepareData();
    }

    protected void prepareData() throws Exception {
        prepareJsonSourceTable();
        prepareJsonTargetTable();
        createJsonWritableTable();
        createJsonReadableTable();
    }

    private void prepareJsonSourceTable() throws Exception {
        gpdbJsonSourceTable = new Table("json_source_table", JSON_SOURCE_TABLE_FIELDS);
        gpdbJsonSourceTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(gpdbJsonSourceTable);
        gpdb.copyFromFile(gpdbJsonSourceTable, new File(localDataResourcesFolder
                + "/arenadata/" + JSON_FILE_NAME), null, null, false);
    }

    private void prepareJsonTargetTable() throws Exception {
        gpdbJsonTargetTable = new Table("json_target_table", JSON_SOURCE_TABLE_FIELDS);
        gpdbJsonTargetTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(gpdbJsonTargetTable);
    }

    private void createJsonWritableTable() throws Exception {
        ExternalTable gpdbJsonWritableTable = TableFactory.getPxfJdbcWritableTable(
                "json_write_ext_table",
                JSON_SOURCE_TABLE_FIELDS,
                gpdbJsonTargetTable.getName(),
                "default");
        gpdb.createTableAndVerify(gpdbJsonWritableTable);
    }

    private void createJsonReadableTable() throws Exception {
        ExternalTable gpdbJsonReadableTable = TableFactory.getPxfJdbcReadableTable(
                "json_read_ext_table",
                JSON_SOURCE_TABLE_FIELDS,
                gpdbJsonSourceTable.getName(),
                "default");
        gpdb.createTableAndVerify(gpdbJsonReadableTable);
    }

    @Test(groups = {"arenadata"})
    public void testPostgresJdbcJson() throws Exception {
        runSqlTest("arenadata/jdbc-json");
    }
}
