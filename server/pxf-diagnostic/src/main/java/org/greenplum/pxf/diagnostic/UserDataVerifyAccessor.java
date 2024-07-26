package org.greenplum.pxf.diagnostic;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;

import java.util.StringJoiner;

/**
 * Test class for regression tests that generates rows of data and includes a filter provided by Greenplum.
 * The returned data has 7 columns delimited with DELIMITER property value: 6 columns of different types
 * and the last column with the value of the filter.
 */
public class UserDataVerifyAccessor extends BasePlugin implements Accessor {

    private static final CharSequence NULL = "";
    private String filter;
    private String userDelimiter;

    private int counter = 0;
    private char textColumn = 'A';
    private static final String UNSUPPORTED_ERR_MESSAGE = "UserDataVerifyAccessor does not support write operation";

    @Override
    public boolean openForRead() {

        // TODO allowlist the option
        FilterVerifyFragmentMetadata metadata = context.getFragmentMetadata();
        filter = metadata.getFilter();
        userDelimiter = String.valueOf(context.getGreenplumCSV().getDelimiter());

        return true;
    }

    @Override
    public OneRow readNextObject() {

        // Termination rule
        if (counter >= 10) {
            return null;
        }

        // Generate tuple with user data value as last column.
        String data = new StringJoiner(userDelimiter)
                .add(counter == 0 ? NULL : String.valueOf(textColumn))            // text
                .add(counter == 1 ? NULL : String.valueOf(counter))               // integer
                .add(counter == 2 ? NULL : String.valueOf(counter % 2 == 0))   // boolean
                .add(counter == 3 ? NULL : String.format("%1$d.%1$d1", counter))  // numeric, decimal, real, double precision
                .add(counter == 4 ? NULL : "" + textColumn + textColumn)          // char
                .add(counter == 5 ? NULL : "" + textColumn + textColumn)          // varchar
                .add(counter == 0 ? NULL : "" + textColumn + textColumn)          // bpchar
                .add(counter == 1 ? NULL : String.valueOf(counter))           // int2
                .add(counter == 2 ? NULL : String.valueOf(counter))           // int8
                .add(counter == 3 ? NULL : String.format("%1$d.%1$d1", counter)) // real
                .add(counter == 4 ? NULL : String.format("%1$d.%1$d1", counter)) // float8
                .add(counter == 5 ? NULL : "\\x5a677265656e706c756d5a")       // bytea
                .add(counter == 0 ? NULL : "2023-01-1" + counter % 10)        // date
                .add(counter == 1 ? NULL : "12:34:5" + counter % 10)          // time
                .add(counter == 2 ? NULL : "2023-01-01 12:34:5" + counter % 10)   // timestamp
                .add(counter == 3 ? NULL : "2023-01-01 12:34:56 +00:0" + counter % 7)  // TIMESTAMP_WITH_TIME_ZONE
                .add(counter == 4 ? NULL : "1 hour 3" + counter + " minutes")   // INTERVAL
                .add(counter == 5 ? NULL : "93d8f9c0-c314-447b-8690-60c40facb8a"  + counter % 10)        // UUID
                .add(counter == 0 ? NULL : "\"{\"\"a\"\":" + counter + "}\"")   // JSON
                .add(counter == 1 ? NULL : "\"{\"\"a\"\":" + counter + "}\"")   // JSONB
                .add(counter == 2 ? NULL : "\"{" + counter + "," + (counter + 1) + "}\"")       // INT2ARRAY
                .add(counter == 3 ? NULL : "\"{" + (counter + 2) + "," + (counter + 3) + "}\"") // INT4ARRAY
                .add(counter == 4 ? NULL : "\"{" + (counter + 4) + "," + (counter + 5) + "}\"") // INT8ARRAY
                .add(counter == 5 ? NULL : "\"{" + String.valueOf(counter % 2 == 0) + "," + String.valueOf(counter % 2 == 1) + "," + "null" + "}\"")   // BOOLARRAY
                .add(counter == 0 ? NULL : "\"{" + textColumn + "," + textColumn + "}\"")                                         // TEXTARRAY
                .add(counter == 1 ? NULL : "\"{" + (counter + 1.1f) + "," + (counter + 2.1f) + "}\"")                             // FLOAT4ARRAY
                .add(counter == 2 ? NULL : "\"{" + (counter + 1.1) + "," + (counter + 2.1) + "}\"")                               // FLOAT8ARRAY
                .add(counter == 3 ? NULL : "\"{\\x" + Integer.toHexString(textColumn) + "B," + "\\x" + Integer.toHexString(textColumn + 1) + "B}\"")     // BYTEAARRAY
                .add(counter == 4 ? NULL : "\"{" + textColumn + textColumn + "," + textColumn + textColumn + "}\"")               // BPCHARARRAY
                .add(counter == 5 ? NULL : "\"{" + textColumn + textColumn + "," + (textColumn + 1) + "}\"")                      // VARCHARARRAY
                .add(counter == 0 ? NULL : "\"{2023-01-01,2023-01-02}\"")                                                         // DATEARRAY
                .add(counter == 1 ? NULL : "\"{93d8f9c0-c314-447b-8690-60c40facb8a5, a56bc0c8-2128-4269-9ce5-cd9c102227b0}\"")     // UUIDARRAY
                .add(counter == 2 ? NULL : "\"{" + (counter + 1.1) + "," + (counter + 2.1) + "}\"")                                                                      // NUMERICARRAY
                .add(counter == 3 ? NULL : "\"{12:00:00,13:00:00}\"")                                                             // TIMEARRAY
                .add(counter == 4 ? NULL : "\"{2023-01-01 12:00:00,2023-01-02 12:00:00}\"")                                       // TIMESTAMPARRAY
                .add(counter == 5 ? NULL : "\"{2023-01-01 12:00:00+00:00,2023-01-02 12:00:00+00:00}\"")                           // TIMESTAMP_WITH_TIMEZONE_ARRAY
                .add(counter == 0 ? NULL : "\"{1 hour,2 hours}\"")                                                                // INTERVALARRAY
                .add(counter == 1 ? NULL : "\"{\"\"{\\\"\"key\\\"\":\\\"\"value\\\"\"}\"\", \"\"{\\\"\"key\\\"\":\\\"\"value\\\"\"}\"\"}\"")   // JSONARRAY
                .add(counter == 2 ? NULL : "\"{\"\"{\\\"\"a\\\"\":" + counter + "}\"\"}\"")   // JSONBARRAY
                .add(filter)                                                                            // Filter
                .toString();
        String key = Integer.toString(counter);

        counter++;
        textColumn++;

        return new OneRow(key, data);
    }

    @Override
    public void closeForRead() {
    }

    @Override
    public boolean openForWrite() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }

    @Override
    public boolean writeNextObject(OneRow onerow) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }

    @Override
    public void closeForWrite() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MESSAGE);
    }
}
