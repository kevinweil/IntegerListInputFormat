An adaptation of codazzo's MultiRowInputFormat at http://github.com/codazzo/MultiRow.

This input format splits a range of integers into any number of input splits for use in a Hadoop job.  This is useful when
you need to, for example, crawl an id space.  If you want to act in parallel on input values from 19 to 500 million with 917 mappers, 
you would configure as follows:

1. In your main/run method of your Hadoop job driver class, add

<pre><code>
Job job = new Job(new Configuration());
...

job.setInputFormatClass(IntegerListInputFormat.class);

IntegerListInputFormat.setListInterval(19, 500000000);
IntegerListInputFormat.setNumSplits(917);
</code></pre>

2. Then, make your mapper take a LongWritable as the key and a NullWritable as the value:

<pre><code>
public static class MyMapper extends Mapper&lt;LongWritable, NullWritable, ..., ...&gt; {
    protected void map(LongWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        // Do something with the id.
        long id = key.get();
        ...
    }
}
</code></pre>

The LongWritable key is the input value.  That's it!
