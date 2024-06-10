package org.greenplum.pxf.plugins.hdfs.parquet.group;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.*;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;

/*
Taken from org.apache.parquet.example.data.simple.
So much hassle just because of private getValue() in SimpleGroup.
 */
public class PlainGroup extends Group {
    protected final GroupType schema;
    protected final List<Object>[] data;

    @SuppressWarnings("unchecked")
    public PlainGroup(GroupType schema) {
        this.schema = schema;
        this.data = new List[schema.getFields().size()];
        for (int i = 0; i < schema.getFieldCount(); i++) {
            this.data[i] = new ArrayList<>();
        }
    }

    @Override
    public void add(int fieldIndex, int value) {
        add(fieldIndex, new IntegerValue(value));
    }

    @Override
    public void add(int fieldIndex, long value) {
        add(fieldIndex, new LongValue(value));
    }

    @Override
    public void add(int fieldIndex, String value) {
        add(fieldIndex, new BinaryValue(Binary.fromString(value)));
    }

    @Override
    public void add(int fieldIndex, boolean value) {
        add(fieldIndex, new BooleanValue(value));
    }

    @Override
    public void add(int fieldIndex, NanoTime value) {
        add(fieldIndex, value.toBinary());
    }

    @Override
    public void add(int fieldIndex, Binary value) {
        add(fieldIndex, new BinaryValue(value));
    }

    @Override
    public void add(int fieldIndex, float value) {
        add(fieldIndex, new FloatValue(value));
    }

    @Override
    public void add(int fieldIndex, double value) {
        add(fieldIndex, new DoubleValue(value));
    }

    @Override
    public void add(int fieldIndex, Group value) {
        // complex types are not yet implemented
        throw new UnsupportedOperationException();
    }

    @Override
    public PlainGroup addGroup(int fieldIndex) {
        PlainGroup g = new PlainGroup(schema.getType(fieldIndex).asGroupType());
        add(fieldIndex, g);
        return g;
    }

    public void add(int fieldIndex, Primitive value) {
        Type type = schema.getType(fieldIndex);
        List<Object> list = data[fieldIndex];
        if (!type.isRepetition(Type.Repetition.REPEATED)
                && !list.isEmpty()) {
            throw new IllegalStateException("field "+fieldIndex+" (" + type.getName() + ") can not have more than one value: " + list);
        }
        list.add(value);
    }

    @Override
    public int getFieldRepetitionCount(int fieldIndex) {
        List<Object> list = data[fieldIndex];
        return list == null ? 0 : list.size();
    }

    @Override
    public Group getGroup(int fieldIndex, int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(int fieldIndex, int index) {
        return ((BinaryValue)getValue(fieldIndex, index)).getString();
    }

    @Override
    public int getInteger(int fieldIndex, int index) {
        return ((IntegerValue)getValue(fieldIndex, index)).getInteger();
    }

    @Override
    public long getLong(int fieldIndex, int index) {
        return ((LongValue)getValue(fieldIndex, index)).getLong();
    }

    @Override
    public double getDouble(int fieldIndex, int index) {
        return ((DoubleValue)getValue(fieldIndex, index)).getDouble();
    }

    @Override
    public float getFloat(int fieldIndex, int index) {
        return ((FloatValue)getValue(fieldIndex, index)).getFloat();
    }

    @Override
    public boolean getBoolean(int fieldIndex, int index) {
        return ((BooleanValue)getValue(fieldIndex, index)).getBoolean();
    }

    @Override
    public Binary getBinary(int fieldIndex, int index) {
        return ((BinaryValue)getValue(fieldIndex, index)).getBinary();
    }

    @Override
    public Binary getInt96(int fieldIndex, int index) {
        return ((BinaryValue)getValue(fieldIndex, index)).getBinary();
    }

    @Override
    public String getValueToString(int fieldIndex, int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GroupType getType() {
        return schema;
    }

    protected Object getValue(int fieldIndex, int index) {
        List<Object> list;
        try {
            list = data[fieldIndex];
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("not found " + fieldIndex + "(" + schema.getFieldName(fieldIndex) + ") in group:\n" + this);
        }
        try {
            return list.get(index);
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("not found " + fieldIndex + "(" + schema.getFieldName(fieldIndex) + ") element number " + index + " in group:\n" + this);
        }
    }

    public Primitive getPrimitive(int fieldIndex, int index) {
        return (Primitive) getValue(fieldIndex, index);
    }

    public PlainGroup getPrimitiveGroup(int fieldIndex, int index) {
        return (PlainGroup) getValue(fieldIndex, index);
    }

    @Override
    public void writeValue(int field, int index, RecordConsumer recordConsumer) {
        (getPrimitive(field, index)).writeValue(recordConsumer);
    }
}