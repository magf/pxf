package org.greenplum.pxf.plugins.hdfs.parquet;

import lombok.experimental.UtilityClass;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.greenplum.pxf.plugins.hdfs.ParquetFileAccessor;
import org.greenplum.pxf.plugins.hdfs.utilities.DecimalUtilities;

@UtilityClass
public class ParquetFixedLenByteArrayUtilities {
    public static byte[] convertFromBigDecimal(DecimalUtilities decimalUtilities, String value, String columnName,
                                               DecimalLogicalTypeAnnotation typeAnnotation) {
        // From org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter.DecimalDataWriter#decimalToBinary
        int precision = Math.min(HiveDecimal.MAX_PRECISION, typeAnnotation.getPrecision());
        int scale = Math.min(HiveDecimal.MAX_SCALE, typeAnnotation.getScale());

        HiveDecimal hiveDecimal = decimalUtilities.parseDecimalStringWithHiveDecimal(value, precision, scale, columnName);
        if (hiveDecimal == null) {
            return null;
        }

        byte[] decimalBytes = hiveDecimal.bigIntegerBytesScaled(scale);

        // Estimated number of bytes needed.
        int precToBytes = ParquetFileAccessor.PRECISION_TO_BYTE_COUNT[precision - 1];
        if (precToBytes == decimalBytes.length) {
            // No padding needed.
            return decimalBytes;
        }

        byte[] tgt = new byte[precToBytes];
        if (hiveDecimal.signum() == -1) {
            // For negative number, initializing bits to 1
            for (int i = 0; i < precToBytes; i++) {
                tgt[i] |= (byte) 0xFF;
            }
        }
        System.arraycopy(decimalBytes, 0, tgt, precToBytes - decimalBytes.length, decimalBytes.length); // Padding leading zeroes/ones.
        return tgt;
        // end -- org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter.DecimalDataWriter#decimalToBinary
    }
}
