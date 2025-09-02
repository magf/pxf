package org.greenplum.pxf.automation.features.hive;

import io.qameta.allure.Feature;
import io.qameta.allure.Step;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.junit.Ignore;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Feature("Hive ORC")
public class HiveOrcTest extends HiveBaseTest {
    private static final String ORC_LARGE_DATA_TYPE = "struct<col1:int,col2:string>";
    private static final String[] HIVE_ORC_LARGE_DATA_COLS = {
            "col1 int",
            "col2 string",
    };

    private static final String[] PXF_ORC_LARGE_DATA_COLS = {
            "col1 int",
            "col2 text",
    };

    private HiveTable hiveOrcSmallDataTable;
    private HiveTable hiveOrcTypesTable;
    private HiveTable hiveOrcPartitionedTable;
    private HiveTable hiveOrcCollectionTable;
    private HiveTable hiveBinaryOrcDataTable;
    private HiveTable hiveOrcLargeDataTable;

    @Override
    @Step("Create external table")
    protected void createExternalTable(String tableName, String[] fields, HiveTable hiveTable) throws Exception {

        exTable = TableFactory.getPxfHiveOrcReadableTable(tableName, fields, hiveTable, true);
        createTable(exTable);
    }

    @Step("Create external vectorized table")
    protected void createExternalVectorizedTable(String tableName, String[] fields, HiveTable hiveTable) throws Exception {

        exTable = TableFactory.getPxfHiveVectorizedOrcReadableTable(tableName, fields, hiveTable, true);
        createTable(exTable);
    }

    @Override
    @Step("Prepare data")
    void prepareData() throws Exception {

        prepareSmallData();
        prepareTypesData();
        prepareOrcData();
        prepareNonDefaultSchemaData();
        preparePxfHiveOrcTypes();
        preparePxfHiveSmallData();
    }

    @Override
    @Step("Prepare small data")
    void prepareSmallData() throws Exception {

        super.prepareSmallData();
        // Create a copy of small data in ORC format
        hiveOrcSmallDataTable = new HiveTable(HIVE_SMALL_DATA_TABLE + "_orc", HIVE_SMALLDATA_COLS);
        hiveOrcSmallDataTable.setStoredAs(ORC);
        hive.createTableAndVerify(hiveOrcSmallDataTable);
        hive.insertData(hiveSmallDataTable, hiveOrcSmallDataTable);
    }

    @Override
    @Step("Prepare binary data")
    void prepareBinaryData() throws Exception {

        super.prepareBinaryData();
        // Create a copy of binary data in ORC format
        hiveBinaryOrcDataTable = new HiveTable(HIVE_BINARY_TABLE + "_orc", HIVE_TYPES_BINARY);
        hiveBinaryOrcDataTable.setStoredAs(ORC);
        hive.createTableAndVerify(hiveBinaryOrcDataTable);
        hive.runQuery("SET hive.vectorized.execution.enabled = false");
        hive.insertData(hiveBinaryTable, hiveBinaryOrcDataTable);
    }

    @Override
    @Step("Prepare hive collection")
    void prepareHiveCollection() throws Exception {

        super.prepareHiveCollection();
        // Create a copy of collection data in ORC format
        hiveOrcCollectionTable = new HiveTable(HIVE_COLLECTIONS_TABLE + "_orc", HIVE_COLLECTION_COLS);
        hiveOrcCollectionTable.setStoredAs(ORC);
        hive.createTableAndVerify(hiveOrcCollectionTable);
        hive.insertData(hiveCollectionTable, hiveOrcCollectionTable);
    }

    @Step("Prepare partitioned data")
    private void preparePartitionedData() throws Exception {

        hiveOrcPartitionedTable = new HiveTable(HIVE_PARTITIONED_TABLE, HIVE_RC_COLS);
        hiveOrcPartitionedTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hiveOrcPartitionedTable.setStoredAs(ORC);
        hive.createTableAndVerify(hiveOrcPartitionedTable);

        hive.runQuery("SET hive.exec.dynamic.partition = true");
        hive.runQuery("SET hive.exec.dynamic.partition.mode = nonstrict");
        // Insert into table using dynamic partitioning.
        // Some of the fields are NULL so they will be inserted into the default partition.
        hive.insertDataToPartition(hiveTypesTable, hiveOrcPartitionedTable,
                new String[]{"fmt"}, new String[]{"t1", "t2", "num1", "t1", "vc1"});
    }

    @Step("Prepare PXF Hive ORC types")
    private void preparePxfHiveOrcTypes() throws Exception {

        createExternalTable(PXF_HIVE_ORC_TABLE,
                PXF_HIVE_TYPES_COLS, hiveOrcAllTypes);
    }

    @Step("Prepare PXF Hive small data")
    private void preparePxfHiveSmallData() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveOrcTable);

        Table gpdbNativeTable = new Table(GPDB_SMALL_DATA_TABLE, PXF_HIVE_SMALLDATA_COLS);
        gpdbNativeTable.setDistributionFields(new String[]{"t1"});
        gpdb.createTableAndVerify(gpdbNativeTable);
        gpdb.copyData(exTable, gpdbNativeTable);
    }

    @Step("Prepare ORC large data")
    private void prepareOrcLargeData(int numRows) throws Exception {
        hiveOrcLargeDataTable = new HiveTable("hive_orc_large_data", HIVE_ORC_LARGE_DATA_COLS);
        hiveOrcLargeDataTable.setStoredAs(ORC);
        hive.createTableAndVerify(hiveOrcLargeDataTable);
        Configuration configuration = new Configuration();
        configuration.set("orc.overwrite.output.file", "true");
        TypeDescription schema = TypeDescription.fromString(ORC_LARGE_DATA_TYPE);
        String fileName = String.format("large_data_%d_rows.orc", numRows);
        String largeOrcDataFile = dataTempFolder + "/" + fileName;
        Writer writer = OrcFile.createWriter(new Path(largeOrcDataFile),
                OrcFile.writerOptions(configuration).setSchema(schema));

        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector col1 = (LongColumnVector) batch.cols[0];
        BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];

        for (int i = 0; i < numRows; i++) {
            int row = batch.size++;

            byte[] bytes = String.format("row%d", i).getBytes(StandardCharsets.UTF_8);
            col1.vector[row] = i;
            col2.setRef(row, bytes, 0, bytes.length);

            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }

        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();

        String destPath = hdfs.getWorkingDirectory() + "/" + fileName;
        hdfs.copyFromLocal(largeOrcDataFile, destPath);
        hive.loadData(hiveOrcLargeDataTable, destPath, false);
    }

    /**
     * Query for small data hive table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void sanity() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE + "_orc",
                PXF_HIVE_SMALLDATA_COLS, hiveOrcSmallDataTable);

        runSqlTest("features/hive/small_data_orc");
        runSqlTest("features/hcatalog/small_data");
    }

    /**
     * Create a Greengage table with a subset of columns from the original
     * Hive table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void columnSubsetOfHiveSchema() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SUBSET_COLS, hiveOrcSmallDataTable);

        runSqlTest("features/hive/column_subset");
    }

    /**
     * Create a Greengage table with a subset of columns from the original
     * partitioned Hive table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void columnSubsetOfPartitionedHiveSchema() throws Exception {

        preparePartitionedData();
        // Create PXF Table using HiveOrc profile
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SUBSET_FMT_COLS, hiveOrcPartitionedTable);

        runSqlTest("features/hive/column_subset_partitioned_table_orc");
    }

    /**
     * Query for binary hive table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hiveLongBinaryType() throws Exception {

        prepareBinaryData();
        createExternalTable(PXF_HIVE_BINARY_TABLE + "_orc",
                new String[]{"b1 BYTEA"}, hiveBinaryOrcDataTable);

        runSqlTest("features/hive/binary_orc_data");
    }

    /**
     * PXF on Hive ORC format table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void storeAsOrc() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveOrcTable);

        runSqlTest("features/hive/small_data");
        runSqlTest("features/hcatalog/small_data_orc");
    }

    /**
     * PXF on Hive ORC format all hive primitive types (all types
     * supported on both Hive 1 & 2)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void storeAsOrcAllTypesHive1AndHive2() throws Exception {

        runSqlTest("features/hive/orc_primitive_types");
        // TODO: hcatalog based access still uses the older Hive profile
        runSqlTest("features/hcatalog/hive_orc_all_types");
    }

    /**
     * PXF on Hive ORC format all hive primitive types (with timestamps;
     * only supported on Hive 1)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void storeAsOrcAllTypesHive1Only() throws Exception {

        runSqlTest("features/hive/orc_primitive_types_hive1_only");
    }

    /**
     * PXF on Hive ORC Queries with ANY, SOME, ALL, and EXISTS
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void queryWithNotPushedDownOperators() throws Exception {

        runSqlTest("features/hive/orc_operators_no_ppd");
    }

    /**
     * PXF on Hive ORC format with Snappy compression table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void storeAsOrcSnappy() throws Exception {

        prepareOrcSnappyData();
        createExternalTable("pxf_hive_orc_snappy",
                PXF_HIVE_SMALLDATA_COLS, hiveOrcSnappyTable);

        runSqlTest("features/hive/orc_snappy");
        runSqlTest("features/hcatalog/hive_orc_snappy");
    }

    /**
     * PXF on Hive ORC format with Zlib compression table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void storeAsOrcZlib() throws Exception {

        prepareOrcZlibData();
        createExternalTable(PXF_HIVE_ORC_ZLIB_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveOrcZlibTable);

        runSqlTest("features/hive/orc_zlib");
        runSqlTest("features/hcatalog/hive_orc_zlib");
    }

    /**
     * PXF on Hive ORC format with multiple ORC backing files
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void storeAsOrcMultiFile() throws Exception {

        prepareOrcMultiFileData();
        createExternalTable(PXF_HIVE_ORC_MULTIFILE_TABLE, PXF_HIVE_SMALLDATA_COLS, hiveOrcMultiFileTable);

        runSqlTest("features/hive/orc_multifile");
    }

    /**
     * PXF with HiveVectorizedORC deprecated profile on Hive ORC format with multiple ORC backing files
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void storeAsOrcMultiFileGetVectorized() throws Exception {

        prepareOrcMultiFileData();
        createExternalVectorizedTable(PXF_HIVE_ORC_MULTIFILE_VECTORIZED_TABLE, PXF_HIVE_SMALLDATA_COLS, hiveOrcMultiFileTable);

        runSqlTest("features/hive/orc_multifile_vectorized");
    }

    /**
     * check default analyze results for pxf external table is as required
     * (pages=1000 tuples=1000000)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features"})
    public void defaultAnalyze() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveSmallDataTable);

        // Perform Analyze on external table and check suitable Warnings.
        gpdb.runQueryWithExpectedWarning("ANALYZE " + exTable.getName(),
                "skipping \"" + exTable.getName() + "\"", true);

        runSqlTest("features/hive/default_analyze");
    }

    /**
     * PXF on Hive table partitioned to one field
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hivePartitionedTable() throws Exception {

        preparePartitionedData();
        // Create PXF Table using HiveOrc profile
        createExternalTable(PXF_HIVE_PARTITIONED_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hiveOrcPartitionedTable);

        runSqlTest("features/hive/hive_partitioned_table_one_format");
    }

    /**
     * Query pxf external table directed to hive orc table contains 3 types of
     * collections (Array, Map and Struct)
     * TODO: Currently the test returns a failure due to HAWQ-1215.
     *       Update the expected response once HAWQ-1215 is fixed.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hiveCollectionTypes() throws Exception {

        prepareHiveCollection();
        createExternalTable(PXF_HIVE_COLLECTIONS_TABLE,
                PXF_HIVE_COLLECTION_COLS, hiveOrcCollectionTable);

        runSqlTest("features/hive/hive_collection_types");
    }

    /**
     * Make sure that PXF works with aggregate queries (including null columns)
     * TODO: enable test when issue is resolved.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "hcatalog"})
    @Ignore
    public void aggregateQueries() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveOrcTypesTable);

        exTable = TableFactory.getPxfHiveOrcReadableTable(PXF_HIVE_SMALL_DATA_TABLE +
                "_multiple_fragments_per_file", PXF_HIVE_TYPES_COLS, hiveOrcAllTypes, true);

        exTable.setAccessor("org.greenplum.pxf.plugins.hive.HiveORCAccessor");
        exTable.setFragmenter("org.greenplum.pxf.automation.testplugin.MultipleHiveFragmentsPerFileFragmenter");
        exTable.setResolver("org.greenplum.pxf.plugins.hive.HiveORCSerdeResolver");
        exTable.setProfile(null);
        exTable.setUserParameters(new String[]{"TEST-FRAGMENTS-NUM=10"});
        createTable(exTable);

        runSqlTest("features/hcatalog/aggregate_queries");
        runSqlTest("features/hive/aggregate_queries");
        runSqlTest("features/hive/aggregate_queries_multiple_fragments_per_file");
    }

    /**
     * Query a small Hive table, skipping the first few rows with skip.header.line.count.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hiveTableWithSkipHeader() throws Exception {

        HiveTable hiveOrcSkipHeaderTable = new HiveTable("hive_table_with_skipheader_orc", HIVE_RC_COLS);
        hiveOrcSkipHeaderTable.setStoredAs(ORC);
        List<List<String>> tableProperties = new ArrayList<>();
        tableProperties.add(Arrays.asList("skip.header.line.count", "3"));
        hiveOrcSkipHeaderTable.setTableProperties(tableProperties);

        hive.createTableAndVerify(hiveOrcSkipHeaderTable);
        hive.insertData(hiveSmallDataTable, hiveOrcSkipHeaderTable);

        createExternalTable("pxf_hive_table_with_skipheader_orc", PXF_HIVE_SMALLDATA_COLS, hiveOrcSkipHeaderTable);

        runSqlTest("features/hive/orc_skip_header_rows");
    }

    @Test(groups = {"hive", "features", "gpdb"})
    public void hiveOrcLargeData() throws Exception {
        prepareOrcLargeData(8192);
        createExternalTable("pxf_hive_orc_large_data", PXF_ORC_LARGE_DATA_COLS, hiveOrcLargeDataTable);
        runSqlTest("features/hive/orc_large_data");
    }

}
