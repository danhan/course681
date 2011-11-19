package org.apache.hadoop.hbase.client.coprocessor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;

public class BixiColumnInterpreter<T, S> implements ColumnInterpreter<T, S> {

  @Override
  public S add(S l1, S l2) {
    return null;
  }

  @Override
  public S castToReturnType(T o) {
    return null;
  }

  @Override
  public int compare(T l1, T l2) {
    return 0;
  }

  @Override
  public double divideForAvg(S o, Long l) {
    return 0;
  }

  @Override
  public T getMaxValue() {
    return null;
  }

  @Override
  public T getMinValue() {
    return null;
  }

  @Override
  public T getValue(byte[] colFamily, byte[] colQualifier, KeyValue kv)
      throws IOException {
    return null;
  }

  @Override
  public S increment(S o) {
    return null;
  }

  @Override
  public S multiply(S o1, S o2) {
    return null;
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
  }

}