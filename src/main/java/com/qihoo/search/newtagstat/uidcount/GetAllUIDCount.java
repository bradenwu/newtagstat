package com.qihoo.search.newtagstat.uidcount;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class GetAllUIDCount extends Configured implements Tool {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GetAllUIDCount(),
				args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("GetAllUIDCount");
			System.err
					.println("Usage: <INPUT_DIR/FILE>...<INPUT_DIR/FILE>  <OUTPUT_DIR> <REDUCE_TASKS_NUM>");
			System.exit(-1);
		}

		Path tempDir = new Path("GetAllUIDCount-temp-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE))); // 定义一个临时目录

		JobConf jobStep1 = new JobConf(getConf(), GetAllUIDCount.class);
		jobStep1.setJobName("GetAllUIDCount-step1");

		jobStep1.setMapOutputKeyClass(Text.class);
		jobStep1.setMapOutputValueClass(IntWritable.class);
		jobStep1.setOutputKeyClass(IntWritable.class);
		jobStep1.setOutputValueClass(IntWritable.class);

		jobStep1.setMapperClass(UIDMapper.class);
		jobStep1.setReducerClass(UIDMiddleReducer.class);

		jobStep1.setInputFormat(TextInputFormat.class);
		jobStep1.setOutputFormat(SequenceFileOutputFormat.class);

		Path[] paths = new Path[args.length - 2];
		for (int i = 0; i < args.length - 2; ++i) {
			paths[i] = new Path(args[i]);
		}

		FileInputFormat.setInputPaths(jobStep1, paths);
		FileOutputFormat.setOutputPath(jobStep1, tempDir);

		int numReduceTasks = Integer.valueOf(args[args.length - 1]);
		jobStep1.setNumReduceTasks(numReduceTasks);

		try {
			JobClient.runJob(jobStep1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			try {
				FileSystem.get(jobStep1).delete(tempDir);
			} catch (IOException ex) {
				// TODO Auto-generated catch block
				ex.printStackTrace();
			}// 删除临时目录
			System.exit(-1);
		}

		JobConf jobStep2 = new JobConf(getConf(), GetAllUIDCount.class);
		jobStep2.setJobName("GetAllUIDCount-step2");

		jobStep2.setMapperClass(IdentityMapper.class);
		jobStep2.setCombinerClass(UIDFinalReducer.class);
		jobStep2.setReducerClass(UIDFinalReducer.class);

		jobStep2.setOutputKeyClass(IntWritable.class);
		jobStep2.setOutputValueClass(IntWritable.class);
		jobStep2.setMapOutputKeyClass(IntWritable.class);
		jobStep2.setMapOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(jobStep2, tempDir);
		jobStep2.setInputFormat(SequenceFileInputFormat.class);
		jobStep2.setOutputFormat(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(jobStep2,
				new Path(args[args.length - 2]));

		jobStep2.setNumReduceTasks(numReduceTasks);
		try {
			JobClient.runJob(jobStep2);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			FileSystem.get(jobStep1).delete(tempDir);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}// 删除临时目录
		return 0;
	}
}
