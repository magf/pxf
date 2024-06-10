package org.greenplum.pxf.plugins.hdfs.parquet.group;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

/*
Taken from org.apache.parquet.example.data.simple.
So much hassle just because of private getValue() in SimpleGroup.
 */
class PlainGroupConverter extends GroupConverter {
  protected final PlainGroupConverter parent;
  protected final int index;
  protected GroupType schema;
  protected PlainGroup current;
  protected Converter[] converters;

  PlainGroupConverter(PlainGroupConverter parent, int index, GroupType schema) {
    this.parent = parent;
    this.index = index;
    this.schema = schema;

    converters = new Converter[schema.getFieldCount()];

    for (int i = 0; i < converters.length; i++) {
      final Type type = schema.getType(i);
      if (type.isPrimitive()) {
        converters[i] = new PlainPrimitiveConverter(this, i);
      } else {
        converters[i] = new PlainGroupConverter(this, i, type.asGroupType());
      }
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    current = (parent != null)?
            parent.getCurrentRecord().addGroup(index) : new PlainGroup(schema);
  }

  @Override
  public void end() {
  }

  public PlainGroup getCurrentRecord() {
    return current;
  }
}