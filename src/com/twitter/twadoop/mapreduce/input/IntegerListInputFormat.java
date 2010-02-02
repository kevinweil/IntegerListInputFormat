package com.twitter.twadoop.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegerListInputFormat extends InputFormat<LongWritable, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(IntegerListInputFormat.class);

  protected static long min_ = 1;
  protected static long max_ = 1;
  protected static long numSplits_ = 1;

  public static void setListInterval(long max) {
    setListInterval(1, max);
  }

  public static void setListInterval(long min, long max) {
    min_ = min;
    max_ = max;
  }

  public static void setNumSplits(long numSplits) {
    numSplits_ = numSplits;
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
      throws IOException, InterruptedException {
    List<InputSplit> splits = Lists.newArrayList();
    // Divide and round up.
    long valuesPerSplit = 1 + (max_ - min_) / numSplits_;
    LOG.info("IntegerListInputFormat creating about " + numSplits_ + " splits for range [" +
             min_ + ", " + max_ + "], about " + valuesPerSplit + " values per split.");

    for (int i = 0; i < numSplits_; ++i) {
      long start = i * valuesPerSplit + min_;
      long stop = Math.min((i + 1) * valuesPerSplit + min_ - 1, max_);
      if (start <= stop) {
        splits.add(new IntegerListInputSplit(start, stop));
      }
    }
    LOG.info("IntegerListInputFormat actually created " + splits.size() + " input splits.");
    return splits;
  }

  @Override
  public RecordReader<LongWritable, NullWritable> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new IntegerListRecordReader();
  }
}
