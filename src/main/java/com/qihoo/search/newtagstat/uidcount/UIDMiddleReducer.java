package com.qihoo.search.newtagstat.uidcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class UIDMiddleReducer extends MapReduceBase implements
		Reducer<Text, IntWritable, IntWritable, IntWritable> {

	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
			throws IOException {

		output.collect(new IntWritable(1), new IntWritable(1));
	}

}
