package org.greenplum.pxf.plugins.hdfs.parquet.group;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

/*
Taken from org.apache.parquet.example.data.simple.
So much hassle just because of private getValue() in SimpleGroup.
 */
public class PlainGroupRecordConverter extends RecordMaterializer<Group> {
  protected final PlainGroupConverter root;

  public PlainGroupRecordConverter(MessageType schema) {
    this.root = new PlainGroupConverter(null, 0, schema);
  }

  @Override
  public Group getCurrentRecord() {
    return getRootConverter().getCurrentRecord();
  }

  @Override
  public PlainGroupConverter getRootConverter() {
    return root;
  }
}