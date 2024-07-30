package org.greenplum.pxf.plugins.hdfs.parquet;

import lombok.experimental.UtilityClass;
import org.apache.parquet.io.api.Binary;

import java.nio.ByteBuffer;
import java.util.UUID;

@UtilityClass
public class ParquetUUIDUtilities {

    public static String readUUID(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long most = bb.getLong();
        long least = bb.getLong();
        return new UUID(most, least).toString();
    }

    public static Binary writeUUID(String str) {
        UUID uuid = UUID.fromString(str);
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return Binary.fromReusedByteArray(bb.array());
    }
}
