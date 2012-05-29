package com.qihoo.search.newtagstat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.InverseMapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UserRequirementTypeStat {
	private static Log logger = LogFactory
			.getLog(UserRequirementTypeStat.class);

	// getLogger(UserRequirementTypeStat.class);

	public static class UserRequirementMaper1 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		String getHost(String url) {
			String result = null;
			if (url.startsWith("http://")) {
				String[] fields = url.split("/");
				if (fields.length >= 3) {
					result = fields[2];
				} else {
					logger.error("url " + url + " split fields.length<3");
				}
			} else {
				logger.error("url " + url + " doesnot startsWith http://");
			}
			return result;
		}

		private static DateFormat sd = new SimpleDateFormat("yyMMdd");

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString().trim();

			// 2dacaf7f8c21d93213a5976c67394363^Ihttp://zjs.cwun.org/Engineer/InitializeRegistor.aspx^I^I1337932783^I水利工程造价工程师注册管理系统^I$
			// 189dc1d152c4bf37647bfbc9fc74cde2^Ihttp://zhidao.baidu.com/question/414508398.html^Ihttp://www.baidu.com/s?word=%C9%ED%B7%DD%D6%A4%BA%C5%C2%EB%BA%CD%D5%E6%CA%B5%D0%D5%C3%FB%B2%E9%D1%AF&tn=utf8speed_dg&f=3^I1337932825
			String[] fields = line.split("\t");
			if (fields.length < 4) {
				logger.error("error! fileds<4, line=" + line);
				System.out.println("error! fileds<4, line=" + line);
			} else {
				String user = fields[0];
				String url = fields[1];
				String host = getHost(url);
				Long timestamp = null;
				String day = null;

				if (host != null) {
					if (fields[3] != null) {
						try {
							timestamp = Long.valueOf(fields[3]);
						} catch (Exception ex) {
							logger.error("error! timestamp = Long.valueOf(fields[3]), line="
									+ line + "fields[3]=" + fields[3]);
						}

						if (timestamp != null) {
							Date date = new Date(timestamp * 1000L);
							day = sd.format(date);
							output.collect(new Text(user), new Text(host + "_"
									+ day));
						} else {
							logger.error("timestamp==null, line=" + line);
						}
					} else {
						logger.error("fields[3]==null, line=" + line);
					}
				} else {
					logger.error("host==null, line=" + line);
				}
			}
		}

	}

	public static class UserRequirementReducer1 extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private static Map<String, String> host2Type = new HashMap<String, String>();
		private static Map<String, String> wildHost2Type = new HashMap<String, String>();
		private static long totalUIDCount = 0;
		private static JobConf jobConf = null;

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Map<String, Set<String>> type2Days = new HashMap<String, Set<String>>();
			String uid = key.toString();
			totalUIDCount++;

			while (values.hasNext()) {
				String hostAndDay = values.next().toString();
				logger.debug("uid=" + uid + ", hostAndDay=" + hostAndDay);
				String[] fields = hostAndDay.split("_");
				if (fields.length == 2) {
					String host = fields[0];
					String day = fields[1];
					if (host2Type.containsKey(host)) {

						logger.debug("host2Type contains host " + host
								+ ", day=" + day);

						String type = host2Type.get(host);
						if (type2Days.containsKey(type)) {
							Set<String> days = type2Days.get(type);
							days.add(day);
						} else {
							Set<String> days = new HashSet<String>();
							days.add(day);
							type2Days.put(type, days);
						}

						logger.debug("type2Days.get(type).size()="
								+ type2Days.get(type).size());

					} else {
						logger.debug("host2Type does not contain host " + host);
					}

				} else {
					logger.error("UserRequirementReducer1.reduce(): fields.length!=2, hostAndDay="
							+ hostAndDay);
					continue;
				}
			}

			for (Entry<String, Set<String>> entry : type2Days.entrySet()) {
				String type = entry.getKey();
				Set<String> days = entry.getValue();
				 logger.debug("type=" + type + ", days.size()=" + days.size()); 
				if (days.size() >= 3) {

					/*
					 * logger.info("days.size()=" + days.size() +
					 * ", output.collect(" + type + ", " + uid + " )");
					 */

					output.collect(new Text(type), new Text(uid));
				}
			}

			// logger.info("output.collect(ALL" + ", " + uid + " )");
			// output.collect(new Text("ALL"), new Text(uid));

		}

		@Override
		public void configure(JobConf job) {
			jobConf = job;
			try {
				FileReader re = new FileReader("host2type.txt");
				BufferedReader reader = new BufferedReader(re);
				String line = null;
				while ((line = reader.readLine()) != null) {
					String[] fields = line.split("\t");
					if (fields.length == 2) {
						String host = fields[0];
						String type = fields[1];
						// *.taobao.com 通配符需单独处理
						if (host.startsWith("*.")) {
							host = host.substring("*.".length());
							logger.debug("wildHost=" + host);
							wildHost2Type.put(host, type);
						} else {
							if (host2Type.containsKey(host)) {
								logger.warn("Duplicate hosts in host2type.txt: "
										+ host);
							} else {
								logger.debug("host2Type.put(" + host + ", "
										+ type + ")");
								host2Type.put(host, type);
							}
						}
					} else {
						logger.error("Wrong Format, line=" + line);
					}

				}
				reader.close();
			} catch (Exception ex) {
				logger.error("exception:" + ex.getMessage());
			}
		}

		@Override
		public void close() throws IOException {
			String outputPath = jobConf.get("mapred.output.dir");
			FileSystem fs = FileSystem.get(jobConf);
			String resultFileName = "totalUIDCount";

			Random random = new Random();
			Path dst = new Path(outputPath + "/" + resultFileName + "_"
					+ random.nextInt());
			/* Path dst = new Path(outputPath + "/" + resultFileName) ; */
			try {
				// FileWrite fw = new FileWriter(dst);
				/*
				 * FileSystem fs = FileSystem.get(URI.create(dst.toString()),
				 * jobConf); FSDataOutputStream out = fs.append(dst);
				 * BufferedWriter br = new BufferedWriter(new
				 * OutputStreamWriter(out));
				 */
				BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
						fs.create(dst, false)));
				br.append(" " + String.valueOf(totalUIDCount) + "\n");
				// br.a
				br.close();
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
		}
	}

	public static class CommonFilePathFilter implements PathFilter {

		static FileSystem fs;
		static Configuration conf;

		static void setConf(Configuration _conf) {
			conf = _conf;
		}

		public boolean accept(Path path) {
			/*
			 * System.out.println("path.toString() = " + path.toString());
			 * System.out.println("path.getName() = " + path.getName());
			 */

			FileStatus fstatus = null;
			try {
				fs = FileSystem.get(conf);
				fstatus = fs.getFileStatus(path);
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (!fstatus.isDir()) {
				boolean result = true;
				if ((path.getName().indexOf("urldatav2") == -1)) {
					/* System.out.println("false!path=" + path.getName()); */
					result = false;
				}
				/* System.out.println("true!path=" + path.getName()); */
				return true;
			} else {
				return true;
			}
		}
	}

	/*
	 * public static class UserRequirementMapper2 extends MapReduceBase
	 * implements Mapper<LongWritable, Text, Text, Text> {
	 *//**
	 * The identify function. Input key/value pair is written directly to
	 * output.
	 */
	/*
	 * public void map(LongWritable key, Text val, OutputCollector<Text, Text>
	 * output, Reporter reporter) throws IOException { String line =
	 * val.toString().trim(); logger.info("output.collect(" + key + ", " + val +
	 * ")"); output.collect(new Text("a"), new Text("1")); } }
	 * 
	 * public static class UserRequirementReducer2 extends MapReduceBase
	 * implements Reducer<Text, Text, Text, Text> {
	 * 
	 * public void reduce(Text type, Iterator<Text> uids, OutputCollector<Text,
	 * Text> output, Reporter reporter) throws IOException {
	 * 
	 * int sum = 0; while (uids.hasNext()) { sum++; }
	 * 
	 * logger.info("output.collect(" + type + ", " + sum + ")");
	 * output.collect(type, new Text(String.valueOf(sum))); } }
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("UserRequirementTypeStat");
			System.err
					.println("Usage: <INPUT_DIR/FILE>...<INPUT_DIR/FILE>  <OUTPUT_DIR> <REDUCE_TASKS_NUM>");
			System.exit(-1);
		}

		/*
		 * Path tempDir = new Path("topurl-temp-" + Integer.toString(new
		 * Random().nextInt(Integer.MAX_VALUE))); // 定义一个临时目录
		 */
		JobConf jobStep1 = new JobConf(UserRequirementTypeStat.class);
		jobStep1.setJobName("UserRequirementTypeStat-step1");

		jobStep1.setOutputKeyClass(Text.class);
		jobStep1.setOutputValueClass(Text.class);

		jobStep1.setMapperClass(UserRequirementMaper1.class);
		jobStep1.setReducerClass(UserRequirementReducer1.class);

		jobStep1.setInputFormat(TextInputFormat.class);
		jobStep1.setOutputFormat(TextOutputFormat.class);

		Path[] paths = new Path[args.length - 2];
		for (int i = 0; i < args.length - 2; ++i) {
			paths[i] = new Path(args[i]);
		}

		FileInputFormat.setInputPaths(jobStep1, paths);
		CommonFilePathFilter.setConf(jobStep1);
		FileInputFormat
				.setInputPathFilter(jobStep1, CommonFilePathFilter.class);
		/* FileOutputFormat.setOutputPath(jobStep1, tempDir); */
		FileOutputFormat.setOutputPath(jobStep1,
				new Path(args[args.length - 2]));

		int numReduceTasks = Integer.valueOf(args[args.length - 1]);
		jobStep1.setNumReduceTasks(numReduceTasks);

		DistributedCache.createSymlink(jobStep1);
		String path = "/user/wuzhigang/host2type.txt";
		Path filePath = new Path(path);
		String uriWithLink = filePath.toUri().toString() + "#"
				+ "host2type.txt";
		DistributedCache.addCacheFile(new URI(uriWithLink), jobStep1);

		JobClient.runJob(jobStep1);

		/*
		 * JobConf jobStep2 = new JobConf(UserRequirementTypeStat.class);
		 * jobStep2.setJobName("UserRequirementTypeStat-step2");
		 * 
		 * jobStep2.setMapperClass(UserRequirementMapper2.class);
		 * jobStep2.setReducerClass(UserRequirementReducer2.class);
		 * 
		 * jobStep2.setOutputKeyClass(Text.class);
		 * jobStep2.setOutputValueClass(Text.class);
		 * jobStep2.setMapOutputKeyClass(Text.class);
		 * jobStep2.setMapOutputValueClass(Text.class);
		 * 
		 * FileInputFormat.setInputPaths(jobStep2, tempDir);
		 * jobStep2.setInputFormat(TextInputFormat.class);
		 * jobStep2.setOutputFormat(TextOutputFormat.class);
		 * FileOutputFormat.setOutputPath(jobStep2, new Path(args[args.length -
		 * 2])); System.out.println("output dir :" + args[args.length-2]);
		 * 
		 * jobStep2.setNumReduceTasks(numReduceTasks);
		 * 
		 * DistributedCache.addCacheFile(new URI(uriWithLink), jobStep2);
		 * JobClient.runJob(jobStep2);
		 * 
		 * FileSystem.get(jobStep1).delete(tempDir);// 删除临时目录
		 */}
}
