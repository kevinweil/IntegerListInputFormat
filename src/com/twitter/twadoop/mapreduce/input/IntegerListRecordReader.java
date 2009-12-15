package com.twitter.twadoop.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegerListRecordReader extends RecordReader<LongWritable, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(IntegerListInputFormat.class);

  // Avoid recreating the object each time getCurrentKey is called.
  protected LongWritable key_ = new LongWritable();
  // Save the split for later use.
  protected IntegerListInputSplit split_;
  // The current position in the split's range.
  protected long cur_;

  public IntegerListRecordReader() {}

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    // Save the split for later.
    split_ = (IntegerListInputSplit)inputSplit;
    // Initialize cur_ to one less than the given min so that it can be
    // consistently incremented in nextKeyValue
    cur_ = split_.getMin() - 1;

    LOG.info("Creating IntegerListRecordReader with InputSplit [" + split_.getMin() + ", " +
             split_.getMax() + "]");    
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    ++cur_;
    key_.set(cur_);
    return cur_ <= split_.getMax();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key_;
  }

  @Override
  public NullWritable getCurrentValue() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (cur_ - split_.getMin() + 1) / (float)split_.getLength();
  }

  @Override
  public void close() throws IOException {}
}
