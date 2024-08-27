package org.greenplum.pxf.plugins.hdfs.parquet;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.io.api.Binary;
import org.postgresql.util.PGInterval;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.SQLException;

@Slf4j
@UtilityClass
public class ParquetIntervalUtilities {
    public static final int INTERVAL_TYPE_LENGTH = 12;
    public static String read(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        int months = bb.getInt();
        int days = bb.getInt();
        int millis = bb.getInt();

        int years = months / 12;
        months -= years * 12;
        int seconds = millis / 1000;
        millis -= seconds * 1000;
        int minutes = seconds / 60;
        seconds -= minutes * 60;
        int hours = minutes / 60;
        minutes -= hours * 60;
        // have to go long way 'cause of float issues
        BigInteger bi = BigInteger.valueOf(seconds * 1000L + millis);
        BigDecimal bd = new BigDecimal(bi, 3);
        PGInterval iv = new PGInterval(years, months, days, hours, minutes, bd.doubleValue());
        return iv.getValue();
    }

    public static Binary write(String str) {
        PGInterval iv;
        try {
            iv = new PGInterval(str);
        } catch (SQLException e) {
            log.error("Couldn't create postgres interval: {}", e.getMessage(), e);
            return null;
        }

        int years = iv.getYears();
        int months = iv.getMonths();
        int days = iv.getDays();
        int hours = iv.getHours();
        int minutes = iv.getMinutes();
        int seconds = iv.getWholeSeconds();
        int micros = iv.getMicroSeconds();
        int total_seconds = (hours * 60 + minutes) * 60 + seconds;

        ByteBuffer bb = ByteBuffer.wrap(new byte[12]);
        bb.putInt(years * 12 + months);
        bb.putInt(days);
        bb.putInt(total_seconds * 1000 + micros / 1000);
        return Binary.fromReusedByteArray(bb.array());
    }
}