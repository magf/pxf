package org.greenplum.pxf.automation.smoke;

import annotations.WorksWithFDW;
import io.qameta.allure.Feature;
import io.qameta.allure.Step;
import jsystem.framework.system.SystemManagerImpl;

import org.greenplum.pxf.automation.components.hive.Hive;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

@Feature("Basic PXF on small Hive table")
public class HiveSmokeTest extends BaseSmoke {
    Hive hive;
    Table dataTable;
    HiveTable hiveTable;
    String fileName = "hiveSmallData.txt";

    @Override
    public void beforeClass() throws Exception {
        // Initialize Hive system object
        hive = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive");
    }

    @Override
    public void afterClass() {
        // close hive connection
        if (hive != null)
            hive.close();
    }

    @Override
    @Step("Prepare data in HDFS and Hive")
    protected void prepareData() throws Exception {
        // Create Hive table
        hiveTable = TableFactory.getHiveByRowCommaTable("hive_table", new String[] {
                "name string",
                "num int",
                "dub double",
                "longNum bigint",
                "bool boolean"
        });
        // hive.dropTable(hiveTable, false);
        hive.createTableAndVerify(hiveTable);
        // Generate Small data, write to HDFS and load to Hive
        Table dataTable = getSmallData();
        hdfs.writeTableToFile((hdfs.getWorkingDirectory() + "/" + fileName), dataTable, ",");

        // load data from HDFS file
        hive.loadData(hiveTable, (hdfs.getWorkingDirectory() + "/" + fileName), false);
    }

    @Override
    @Step("Create GPDB external table directed to Hive")
    protected void createTables() throws Exception {
        exTable = TableFactory.getPxfHiveReadableTable("pxf_smoke_small_data", new String[] {
                "name text",
                "num integer",
                "dub double precision",
                "longNum bigint",
                "bool boolean"
        }, hiveTable, true);
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exTable);
    }

    @Override
    protected void queryResults() throws Exception {
        runSqlTest("smoke/small_data");
        runSqlTest("smoke/hcatalog_small_data");
    }

    @Test(groups = { "smoke" })
    @WorksWithFDW
    public void test() throws Exception {
        runTest();
    }
}
