package org.greenplum.pxf.plugins.hdfs.parquet.group;

import org.apache.parquet.example.data.simple.*;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

class PlainPrimitiveConverter extends PrimitiveConverter {
  protected final PlainGroupConverter parent;
  protected final int index;

  PlainPrimitiveConverter(PlainGroupConverter parent, int index) {
    this.parent = parent;
    this.index = index;
  }

  /**
   * {@inheritDoc}
   * @see org.apache.parquet.io.api.PrimitiveConverter#addBinary(Binary)
   */
  @Override
  public void addBinary(Binary value) {
    parent.getCurrentRecord().add(index, new BinaryValue(value));
  }

  /**
   * {@inheritDoc}
   * @see org.apache.parquet.io.api.PrimitiveConverter#addBoolean(boolean)
   */
  @Override
  public void addBoolean(boolean value) {
    parent.getCurrentRecord().add(index, new BooleanValue(value));
  }

  /**
   * {@inheritDoc}
   * @see org.apache.parquet.io.api.PrimitiveConverter#addDouble(double)
   */
  @Override
  public void addDouble(double value) {
    parent.getCurrentRecord().add(index, new DoubleValue(value));
  }

  /**
   * {@inheritDoc}
   * @see org.apache.parquet.io.api.PrimitiveConverter#addFloat(float)
   */
  @Override
  public void addFloat(float value) {
    parent.getCurrentRecord().add(index, new FloatValue(value));
  }

  /**
   * {@inheritDoc}
   * @see org.apache.parquet.io.api.PrimitiveConverter#addInt(int)
   */
  @Override
  public void addInt(int value) {
    parent.getCurrentRecord().add(index, new IntegerValue(value));
  }

  /**
   * {@inheritDoc}
   * @see org.apache.parquet.io.api.PrimitiveConverter#addLong(long)
   */
  @Override
  public void addLong(long value) {
    parent.getCurrentRecord().add(index, new LongValue(value));
  }
}