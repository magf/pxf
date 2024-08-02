package org.greenplum.pxf.automation.dataschema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class CustomWritableWithChar
  implements Writable
{
  public int int1;
  public char char1;

  public CustomWritableWithChar()
  {
    this.int1 = 0;

    this.char1 = '\n';
  }

  public CustomWritableWithChar(int paramInt1, int paramInt2, int paramInt3)
  {
    this.int1 = paramInt1;
    this.char1 = ((char)paramInt2);
  }

  int GetInt1()
  {
    return this.int1;
  }

  char GetChar()
  {
    return this.char1;
  }

  @Override
public void write(DataOutput paramDataOutput)
    throws IOException
  {
    IntWritable localIntWritable = new IntWritable();

    localIntWritable.set(this.int1);
    localIntWritable.write(paramDataOutput);

    localIntWritable.set(this.char1);
    localIntWritable.write(paramDataOutput);
  }

  @Override
public void readFields(DataInput paramDataInput)
    throws IOException
  {
    IntWritable localIntWritable = new IntWritable();

    localIntWritable.readFields(paramDataInput);
    this.int1 = localIntWritable.get();

    localIntWritable.readFields(paramDataInput);
    this.char1 = ((char)localIntWritable.get());
  }
}
