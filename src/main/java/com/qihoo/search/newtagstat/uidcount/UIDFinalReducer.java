package com.qihoo.search.newtagstat.uidcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class UIDFinalReducer extends MapReduceBase implements
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	public void reduce(IntWritable _key, Iterator<IntWritable> values,
			OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
			throws IOException {

		int sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}
		output.collect(new IntWritable(1), new IntWritable(sum));
	}

}
