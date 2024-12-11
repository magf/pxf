package org.greenplum.pxf.automation.arenadata;

import io.qameta.allure.Feature;
import io.qameta.allure.Step;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.testng.annotations.Test;
import java.io.File;

import static org.greenplum.pxf.automation.PxfTestConstant.PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE;

@Feature("JDBC features")
public class JdbcFeaturesTest extends BaseFeature {
    private static final String[] LONG_YEAR_SOURCE_TABLE_FIELDS = new String[]{
            "id    int",
            "birth_date   date",
            "birth_date_dad  timestamp",
            "birth_date_mom  timestamp with time zone"};

    private static final String[] BOOL_DATA_TYPE_SOURCE_TABLE_FIELDS = new String[]{
            "id    int",
            "v_text   text",
            "v_bool  bool"};

    // Writable table doesn't support 'timestamp with time zone'
    private static final String[] LONG_YEAR_TARGET_TABLE_FIELDS = new String[]{
            "id    int",
            "birth_date   date",
            "birth_date_dad  timestamp"};

    private static final String[] NAMED_QUERY_TABLE_FIELDS = new String[]{
            "id  int",
            "name text"};

    private static final String NAMED_QUERY_SERVER_PROFILE = "named";
    private static final String LONG_YEAR_DATA_FILE_NAME = "long_year.txt";
    private static final String PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH = "templates/named-query/jdbc-site.xml";
    private static final String PXF_NAMED_QUERY_TEMPLATE_RELATIVE_PATH = "templates/named-query/named_query.sql";
    private Table gpdbLongYearSourceTable;
    private Table gpdbLongYearTargetTable;
    private Table gpdbLongYearTargetLegacyTable;
    private Table gpdbBoolDateTypeSourceTable;

    @Override
    protected void beforeClass() throws Exception {
        if (!FDWUtils.useFDW) {
            String pxfHome = cluster.getPxfHome();
            String pxfJdbcSiteConfPath = String.format(PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE, pxfHome, NAMED_QUERY_SERVER_PROFILE);
            String queryFile = pxfHome + "/" + PXF_NAMED_QUERY_TEMPLATE_RELATIVE_PATH;
            String pxfJdbcSiteConfTemplate = pxfHome + "/" + PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH;
            cluster.copyFileToNodes(pxfJdbcSiteConfTemplate, pxfJdbcSiteConfPath, true, false);
            cluster.copyFileToNodes(queryFile, pxfJdbcSiteConfPath, true, false);
            prepareData();
        }
    }

    @Step("Prepare data")
    protected void prepareData() throws Exception {
        prepareLongYearSourceTable();
        prepareLongYearTargetTable();
        prepareLongYearTargetLegacyTable();
        prepareBoolDataTypeSourceTable();
        prepareNamedQuerySourceTable();
        createLongYearReadableTable();
        createLongYearReadableLegacyTable();
        createLongYearWritableTable();
        createLongYearWritableLegacyTable();
        createBoolDataTypeReadableTable();
        createNamedQueryExternalTable("named_query_read_ext_table", "query:named_query");
        createNamedQueryExternalTable("named_query_wrong_read_ext_table", "query:wrong_file_name");
    }

    @Step("Prepare long year source table")
    private void prepareLongYearSourceTable() throws Exception {
        gpdbLongYearSourceTable = new Table("long_year_source_table", LONG_YEAR_SOURCE_TABLE_FIELDS);
        gpdbLongYearSourceTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(gpdbLongYearSourceTable);
        gpdb.copyFromFile(gpdbLongYearSourceTable, new File(localDataResourcesFolder
                + "/arenadata/" + LONG_YEAR_DATA_FILE_NAME), "E','", "E'\\\\N'", true);

    }

    @Step("Prepare long year target table")
    private void prepareLongYearTargetTable() throws Exception {
        gpdbLongYearTargetTable = new Table("long_year_target_table", LONG_YEAR_TARGET_TABLE_FIELDS);
        gpdbLongYearTargetTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(gpdbLongYearTargetTable);
    }

    @Step("Prepare long year target legacy table")
    private void prepareLongYearTargetLegacyTable() throws Exception {
        gpdbLongYearTargetLegacyTable = new Table("long_year_target_legacy_table", LONG_YEAR_TARGET_TABLE_FIELDS);
        gpdbLongYearTargetLegacyTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(gpdbLongYearTargetLegacyTable);
    }

    @Step("Create long year readable table")
    private void createLongYearReadableTable() throws Exception {
        ExternalTable gpdbReadableTable = TableFactory.getPxfJdbcReadableTable(
                "long_year_read_ext_table",
                LONG_YEAR_SOURCE_TABLE_FIELDS,
                gpdbLongYearSourceTable.getName(),
                "default");
        gpdbReadableTable.setUserParameters(new String[]{"date_wide_range=true"});
        gpdb.createTableAndVerify(gpdbReadableTable);
    }

    @Step("Create long year readable legacy table")
    private void createLongYearReadableLegacyTable() throws Exception {
        ExternalTable gpdbReadableTable = TableFactory.getPxfJdbcReadableTable(
                "long_year_read_legacy_ext_table",
                LONG_YEAR_SOURCE_TABLE_FIELDS,
                gpdbLongYearSourceTable.getName(),
                "default");
        gpdbReadableTable.setUserParameters(new String[]{"jdbc.date.wide-range=true"});
        gpdb.createTableAndVerify(gpdbReadableTable);
    }

    @Step("Create long year writable table")
    private void createLongYearWritableTable() throws Exception {
        ExternalTable gpdbWritableTable = TableFactory.getPxfJdbcWritableTable(
                "long_year_write_ext_table",
                LONG_YEAR_TARGET_TABLE_FIELDS,
                gpdbLongYearTargetTable.getName(),
                "default");
        gpdbWritableTable.setUserParameters(new String[]{"date_wide_range=true"});
        gpdb.createTableAndVerify(gpdbWritableTable);
    }

    @Step("Create long year writable legacy table")
    private void createLongYearWritableLegacyTable() throws Exception {
        ExternalTable gpdbWritableTable = TableFactory.getPxfJdbcWritableTable(
                "long_year_write_legacy_ext_table",
                LONG_YEAR_TARGET_TABLE_FIELDS,
                gpdbLongYearTargetLegacyTable.getName(),
                "default");
        gpdbWritableTable.setUserParameters(new String[]{"jdbc.date.wide-range=true"});
        gpdb.createTableAndVerify(gpdbWritableTable);
    }

    @Step("Prepare bool data type source table")
    private void prepareBoolDataTypeSourceTable() throws Exception {
        gpdbBoolDateTypeSourceTable = new Table("bool_data_type_source_table", BOOL_DATA_TYPE_SOURCE_TABLE_FIELDS);
        gpdbBoolDateTypeSourceTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(gpdbBoolDateTypeSourceTable);
        String[][] rows = new String[][]{
                {"1", "text1", "false"},
                {"2", "text2", "false"},
                {"3", "text3", "false"},
                {"4", "text4", "false"},
                {"5", "text5", "false"}};
        Table dataTable = new Table("dataTable", BOOL_DATA_TYPE_SOURCE_TABLE_FIELDS);
        dataTable.addRows(rows);
        gpdb.insertData(dataTable, gpdbBoolDateTypeSourceTable);
    }

    @Step("Create bool data type readable table")
    private void createBoolDataTypeReadableTable() throws Exception {
        Table gpdbBoolDataTypeReadableTable = TableFactory.getPxfJdbcReadableTable(
                "bool_data_type_read_ext_table",
                BOOL_DATA_TYPE_SOURCE_TABLE_FIELDS,
                gpdbBoolDateTypeSourceTable.getName(),
                "default");
        gpdb.createTableAndVerify(gpdbBoolDataTypeReadableTable);
    }

    @Step("Prepare named query source table")
    private void prepareNamedQuerySourceTable() throws Exception {
        Table gpdbNamedSourceTable = new Table("gpdb_named_query_source_table", NAMED_QUERY_TABLE_FIELDS);
        gpdbNamedSourceTable.setDistributionFields(new String[]{"name"});
        gpdb.createTableAndVerify(gpdbNamedSourceTable);
        String[][] rows = new String[][]{
                {"1", "text1"},
                {"2", "text2"},
                {"3", "text3"}};
        Table dataTable = new Table("dataTable", NAMED_QUERY_TABLE_FIELDS);
        dataTable.addRows(rows);
        gpdb.insertData(dataTable, gpdbNamedSourceTable);
    }

    @Step("Create named query external table")
    private void createNamedQueryExternalTable(String tableName, String dataSourcePath) throws Exception {
        ExternalTable pxfJdbcNamedQuery = TableFactory.getPxfJdbcReadableTable(
                tableName,
                NAMED_QUERY_TABLE_FIELDS,
                dataSourcePath,
                NAMED_QUERY_SERVER_PROFILE);
        pxfJdbcNamedQuery.setHost(pxfHost);
        pxfJdbcNamedQuery.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcNamedQuery);
    }

    @Test(groups = {"arenadata"})
    public void testLongYear() throws Exception {
        runSqlTest("arenadata/long-year");
    }

    @Test(groups = {"arenadata"})
    public void testBoolDataType() throws Exception {
        runSqlTest("arenadata/bool-data");
    }

    @Test(groups = {"arenadata"})
    public void testJdbcCloseSession() throws Exception {
        runSqlTest("arenadata/jdbc-close-session");
    }
}
