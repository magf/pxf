package org.greenplum.pxf.plugins.hbase;

import org.apache.hadoop.hbase.HRegionInfo;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

class HBaseFragmentMetadataTest {

    @Test
    public void testConstructor() {
        final byte[] startKey = new byte[0];
        final byte[] endKey = new byte[0];
        final byte[] fooValue = new byte[0];
        Map<String, byte[]> columnMapping = new HashMap<>();
        columnMapping.put("foo", fooValue);

        HBaseFragmentMetadata metadata = new HBaseFragmentMetadata(startKey, endKey, columnMapping);
        assertNotNull(metadata);
        assertSame(startKey, metadata.getStartKey());
        assertSame(endKey, metadata.getEndKey());
        assertSame(columnMapping, metadata.getColumnMapping());
        assertSame(fooValue, metadata.getColumnMapping().get("foo"));
    }
}