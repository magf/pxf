package org.greenplum.pxf.automation.arenadata;

import annotations.WorksWithFDW;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.automation.features.BaseWritableFeature;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;
import parquet.example.data.Group;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@WorksWithFDW
public class PxfHdfsParquetTimezoneTest extends BaseWritableFeature {

    private static final String[] PARQUET_TIMESTAMP_COLUMN = new String[]{
            "tmp        TIMESTAMP"
    };
    private static final String TIMESTAMP_DATA = "('2022-06-22 19:10:25')";
    private static final String PARQUET_FORMAT = "parquet";
    private static final String USE_INT64_TIMESTAMPS_PARAM = "USE_INT64_TIMESTAMPS=";
    private static final String USE_LOCAL_PXF_TIMEZONE_WRITE_PARAM = "USE_LOCAL_PXF_TIMEZONE_WRITE=";
    private static final String USE_LOCAL_PXF_TIMEZONE_READ_PARAM = "USE_LOCAL_PXF_TIMEZONE_READ=";


    @Test(groups = {"arenadata"})
    public void testUseInt64TimestampsTrue() throws Exception {
        String writeTableName = "writable_test_table";
        String hdfsPath = hdfsWritePath + "test";
        String useInt64Param = USE_INT64_TIMESTAMPS_PARAM + true;
        writableExTable = TableFactory.getPxfHcfsWritableTable(writeTableName, PARQUET_TIMESTAMP_COLUMN, hdfsPath, hdfs.getBasePath(), PARQUET_FORMAT);
        writableExTable.setUserParameters(new String[]{useInt64Param});
        createTable(writableExTable);
        gpdb.insertData(TIMESTAMP_DATA, writableExTable);
        List<String> hdfsPathFiles = hdfs.list(hdfsPath);
        assertEquals(1, hdfsPathFiles.size());
        String parquetPath = hdfsPathFiles.get(0);
        ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(
                hdfs.getConfiguration(), new Path(parquetPath), ParquetMetadataConverter.NO_FILTER);
        MessageType parquetData = parquetMetadata.getFileMetaData().getSchema();
        List<Type> parquetSchema = parquetData.getFields();
        assertEquals(1, parquetSchema.size());
        Type tmpType = parquetSchema.get(0);
        assertEquals("tmp", tmpType.getName());
        assertTrue(tmpType.isPrimitive());
        assertEquals("INT64", ((PrimitiveType) tmpType).getPrimitiveTypeName().toString());
        String readTableName = "readable_test_table";
        readableExTable = TableFactory.getPxfHcfsReadableTable(readTableName, PARQUET_TIMESTAMP_COLUMN, hdfsPath, hdfs.getBasePath(), PARQUET_FORMAT);
        createTable(readableExTable);
        runSqlTest("arenadata/parquet/readable");
    }

    private String getParquetPath(String hdfsFolderPath) throws Exception {
        return hdfs.list(hdfsFolderPath).stream()
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No parquet file is found in this path"));
    }
}
