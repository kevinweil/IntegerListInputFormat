package com.twitter.twadoop.mapreduce.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegerListInputSplit extends InputSplit implements Writable {
  private static final Logger LOG = LoggerFactory.getLogger(IntegerListInputFormat.class);

  protected long min_;
  protected long max_;

  public IntegerListInputSplit() {}

  public IntegerListInputSplit(long min, long max) {
    if (min > max) {
      throw new IllegalArgumentException("Attempt to create IntegerListInputSplit with min > max, min = " +
          min + " and max = " + max);
    }
    LOG.info("Creating IntegerListInputSplit with InputSplit [" + min + ", " + max + "]");    
    min_ = min;
    max_ = max;
  }

  public long getMin() {
    return min_;
  }

  public long getMax() {
    return max_;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return max_ - min_ + 1;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[] {};
  }

  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(min_);
    dataOutput.writeLong(max_);
  }

  public void readFields(DataInput dataInput) throws IOException {
    min_ = dataInput.readLong();
    max_ = dataInput.readLong();
  }
}
