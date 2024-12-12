package org.greenplum.pxf.automation.features.cloud;

import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.minio.Minio;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.testng.annotations.Test;

import java.nio.file.Paths;

import static org.greenplum.pxf.automation.features.tpch.LineItem.LINEITEM_SCHEMA;

/**
 * Functional S3 Select Test
 */
public class S3SelectTest extends BaseFeature {

    private static final String[] PXF_S3_SELECT_INVALID_COLS = {
            "invalid_orderkey       BIGINT",
            "invalid_partkey        BIGINT",
            "invalid_suppkey        BIGINT",
            "invalid_linenumber     BIGINT",
            "invalid_quantity       DECIMAL(15,2)",
            "invalid_extendedprice  DECIMAL(15,2)",
            "invalid_discount       DECIMAL(15,2)",
            "invalid_tax            DECIMAL(15,2)",
            "invalid_returnflag     CHAR(1)",
            "invalid_linestatus     CHAR(1)",
            "invalid_shipdate       DATE",
            "invalid_commitdate     DATE",
            "invalid_receiptdate    DATE",
            "invalid_shipinstruct   CHAR(25)",
            "invalid_shipmode       CHAR(10)",
            "invalid_comment        VARCHAR(44)"
    };


    private static final String sampleCsvFile = "sample.csv";
    private static final String sampleGzippedCsvFile = "sample.csv.gz";
    private static final String sampleBzip2CsvFile = "sample.csv.bz2";
    private static final String sampleCsvNoHeaderFile = "sample-no-header.csv";
    private static final String sampleParquetFile = "sample.parquet";
    private static final String sampleParquetSnappyFile = "sample.snappy.parquet";
    private static final String sampleParquetGzipFile = "sample.gz.parquet";

    private static final String BUCKET_NAME = "pxf-s3-select";

    private Minio minio;

    /**
     * Prepare all server configurations and components
     */
    @Override
    public void beforeClass() throws Exception {
        minio = (Minio) SystemManagerImpl.getInstance().getSystemObject("minio");
        cleanBucket();
    }

    @Override
    protected void afterClass() throws Exception {
        super.afterClass();
        cleanBucket();
    }

    @Test(groups = {"s3", "gpdb"})
    public void testPlainCsvWithHeaders() throws Exception {
        String[] userParameters = {"FILE_HEADER=IGNORE", "S3_SELECT=ON"};
        runTestScenario("csv", "s3", "csv", localDataResourcesFolder + "/s3select/",
                sampleCsvFile, "|", userParameters);
    }

    @Test(groups = {"s3", "gpdb"})
    public void testPlainCsvWithHeadersUsingHeaderInfo() throws Exception {
        String[] userParameters = {"FILE_HEADER=USE", "S3_SELECT=ON"};
        runTestScenario("csv_use_headers", "s3", "csv", localDataResourcesFolder + "/s3select/",
                sampleCsvFile, "|", userParameters);
    }

    @Test(groups = {"s3", "gpdb"})
    public void testCsvWithHeadersUsingHeaderInfoWithWrongColumnNames() throws Exception {
        String[] userParameters = {"FILE_HEADER=USE", "S3_SELECT=ON"};
        runTestScenario("errors/", "csv_use_headers_with_wrong_col_names", "s3", "csv",
                localDataResourcesFolder + "/s3select/", sampleCsvFile, "|", userParameters,
                PXF_S3_SELECT_INVALID_COLS);
    }

    @Test(groups = {"s3", "gpdb"})
    public void testPlainCsvWithNoHeaders() throws Exception {
        String[] userParameters = {"FILE_HEADER=NONE", "S3_SELECT=ON"};
        runTestScenario("csv_noheaders", "s3", "csv",
                localDataResourcesFolder + "/s3select/", sampleCsvNoHeaderFile, "|", userParameters);
    }

    @Test(groups = {"s3", "gpdb"})
    public void testGzipCsvWithHeadersUsingHeaderInfo() throws Exception {
        String[] userParameters = {"FILE_HEADER=USE", "S3_SELECT=ON", "COMPRESSION_CODEC=gzip"};
        runTestScenario("gzip_csv_use_headers", "s3", "csv",
                localDataResourcesFolder + "/s3select/", sampleGzippedCsvFile, "|", userParameters);
    }

    @Test(groups = {"s3", "gpdb"})
    public void testBzip2CsvWithHeadersUsingHeaderInfo() throws Exception {
        String[] userParameters = {"FILE_HEADER=USE", "S3_SELECT=ON", "COMPRESSION_CODEC=bzip2"};
        runTestScenario("bzip2_csv_use_headers", "s3", "csv",
                localDataResourcesFolder + "/s3select/", sampleBzip2CsvFile, "|", userParameters);
    }

    @Test(groups = {"s3", "gpdb"})
    public void testParquet() throws Exception {
        String[] userParameters = {"S3_SELECT=ON"};
        runTestScenario("parquet", "s3", "parquet",
                localDataResourcesFolder + "/s3select/", sampleParquetFile, null, userParameters);
    }

    @Test(groups = {"s3", "gpdb"})
    public void testParquetWildcardLocation() throws Exception {
        String[] userParameters = {"S3_SELECT=ON"};
        runTestScenario("", "parquet", "s3", "parquet",
                localDataResourcesFolder + "/s3select/", sampleParquetFile,
                null, userParameters, LINEITEM_SCHEMA);
    }

    @Test(groups = {"s3", "gpdb"})
    public void testSnappyParquet() throws Exception {
        String[] userParameters = {"S3_SELECT=ON"};
        runTestScenario("parquet_snappy", "s3", "parquet",
                localDataResourcesFolder + "/s3select/", sampleParquetSnappyFile, null, userParameters);
    }

    @Test(groups = {"s3", "gpdb"})
    public void testGzipParquet() throws Exception {
        String[] userParameters = {"S3_SELECT=ON"};
        runTestScenario("parquet_gzip", "s3", "parquet",
                localDataResourcesFolder + "/s3select/", sampleParquetGzipFile, null, userParameters);
    }

    private void runTestScenario(
            String name,
            String server,
            String format,
            String srcPath,
            String fileName,
            String delimiter,
            String[] userParameters)
            throws Exception {

        runTestScenario("",
                name,
                server,
                format,
                srcPath,
                fileName,
                delimiter,
                userParameters,
                LINEITEM_SCHEMA);
    }

    private void runTestScenario(
            String qualifier,
            String name,
            String server,
            String format,
            String srcPath,
            String fileName,
            String delimiter,
            String[] userParameters,
            String[] fields)
            throws Exception {

        String tableName = "s3select_" + name;
        String locationPath = BUCKET_NAME + "/" + tableName + "/" + fileName;
        String serverParam = (server == null) ? null : "server=" + server;

        minio.createBucket(BUCKET_NAME);
        minio.uploadFile(BUCKET_NAME, tableName + "/" + fileName, Paths.get(srcPath + fileName));

        exTable = new ReadableExternalTable(tableName, fields, locationPath, "CSV");
        exTable.setProfile("s3:" + format);
        exTable.setServer(serverParam);

        if (delimiter != null)
            exTable.setDelimiter(delimiter);
        if (userParameters != null)
            exTable.setUserParameters(userParameters);

        gpdb.createTableAndVerify(exTable);

        runSqlTest(String.format("features/s3_select/%s%s", qualifier, name));
    }

    private void cleanBucket() {
        if (minio != null) {
            minio.clean(BUCKET_NAME);
        }
    }
}
