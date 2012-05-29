package com.qihoo.search.newtagstat;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class UserRequirementTypeStat2 {
	private static Log logger = LogFactory
			.getLog(UserRequirementTypeStat2.class);

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString().trim();
			String[] fields = line.split("\t");
			if (fields.length == 2) {
				output.collect(new Text(fields[0]), new IntWritable(1));
			} else if (fields.length == 1) {
				String type = "AllUIDCount";

				Integer count = null;
				try {
					count = Integer.valueOf(fields[0]);
					output.collect(new Text(type), new IntWritable(count));
				} catch (Exception ex) {
					logger.error(ex.getMessage());
				}

			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(UserRequirementTypeStat2.class);
		conf.setJobName("UserRequirementTypeStat2");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		Path[] paths = new Path[args.length - 2];
		for (int i = 0; i < args.length - 2; ++i) {
			paths[i] = new Path(args[i]);
		}
		FileInputFormat.setInputPaths(conf, paths);
		FileOutputFormat.setOutputPath(conf, new Path(args[args.length - 2]));
		int numReduceTasks = Integer.valueOf(args[args.length - 1]);
		conf.setNumReduceTasks(numReduceTasks);

		JobClient.runJob(conf);
	}
}
