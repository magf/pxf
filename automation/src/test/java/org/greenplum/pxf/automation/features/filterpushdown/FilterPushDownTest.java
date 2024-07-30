package org.greenplum.pxf.automation.features.filterpushdown;

import annotations.SkipForFDW;
import annotations.WorksWithFDW;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

/**
 * Functional PXF filter pushdown cases
 */
@WorksWithFDW
public class FilterPushDownTest extends BaseFeature {

    private static final String COMMA = ",";

    private static final String[] FIELDS = new String[]{
        "t0    text",
        "a1    integer",
        "b2    boolean",
        "c3    numeric",
        "d4    char(2)",
        "e5    varchar(2)",
        "x1    bpchar(2)",
        "x2    smallint",
        "x3    bigint",
        "x4    real",
        "x5    float8",
        "x6    bytea",
        "x7    date",
        "x8    time",
        "x9    timestamp",
        "x10   timestamp with time zone",
        "x11   interval",
        "x12   uuid",
        "x13   json",
        "x14   jsonb",
        "x15   int2[]",
        "x16   int4[]",
        "x17   int8[]",
        "x18   bool[]",
        "x19   text[]",
        "x20   float4[]",
        "x21   float8[]",
        "x22   bytea[]",
        "x23   bpchar[]",
        "x24   varchar(2)[]",
        "x25   date[]",
        "x26   uuid[]",
        "x27   numeric[]",
        "x28   time[]",
        "x29   timestamp[]",
        "x30   timestamp with time zone[]",
        "x31   interval[]",
        "x32   json[]",
        "x33   jsonb[]",
        "filterValue  text"
    };

    /**
     * Check that PXF receives the expected filter string, using a table with a comma delimiter
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "security"})
    public void checkFilterPushDown() throws Exception {
        preparePxfTable(COMMA);
        runSqlTest("features/filterpushdown/checkFilterPushDown");
    }

    /**
     * Check that PXF receives no filter string, using a table with a comma delimiter
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "security"})
    @SkipForFDW // the guc used in the test is not applicable to FDW and has no effect
    public void checkFilterPushDownDisabled() throws Exception {
        preparePxfTable(COMMA);
        runSqlTest("features/filterpushdown/checkFilterPushDownDisabled");
    }

    /**
     * Check that PXF receives the expected filter string, using a table with a hexademical delimiter
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "security"})
    public void checkFilterStringHexDelimiter() throws Exception {
        preparePxfTable("E'\\x01'");
        runSqlTest("features/filterpushdown/checkFilterPushDownHexDelimiter");
    }

    /**
     * Prepares a PXF external text table with a given delimiter.
     * @param delimiter delimiter
     * @throws Exception
     */
    private void preparePxfTable(String delimiter) throws Exception {
        // Create PXF external table for filter testing
        exTable = TableFactory.getPxfReadableTestCSVTable("test_filter", FIELDS, "dummy_path", delimiter);
        exTable.setProfile("system:filter"); // use system:filter profile shipped with PXF server
        gpdb.createTableAndVerify(exTable);
    }
}
