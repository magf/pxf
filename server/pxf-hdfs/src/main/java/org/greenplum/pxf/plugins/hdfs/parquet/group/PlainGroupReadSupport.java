package org.greenplum.pxf.plugins.hdfs.parquet.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

/*
Taken from org.apache.parquet.example.data.simple.
So much hassle just because of private getValue() in SimpleGroup.
 */
public class PlainGroupReadSupport extends GroupReadSupport {
  @Override
  public RecordMaterializer<Group> prepareForRead(Configuration configuration,
                                                  Map<String, String> keyValueMetaData, MessageType fileSchema,
                                                  ReadContext readContext) {
    return new PlainGroupRecordConverter(readContext.getRequestedSchema());
  }
}