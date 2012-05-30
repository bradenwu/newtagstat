package com.qihoo.search.newtagstat.uidcount;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class UIDMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {

	org.apache.commons.logging.Log logger = LogFactory.getLog(UIDMapper.class);

	private Set<String> uidSet = new HashSet<String>();
	private final int MAX_SIZE = 10000000;// 32B*10*10000000~3GB

	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String line = value.toString().trim();
		// 2dacaf7f8c21d93213a5976c67394363^Ihttp://zjs.cwun.org/Engineer/InitializeRegistor.aspx^I^I1337932783^I水利工程造价工程师注册管理系统^I$
		// 189dc1d152c4bf37647bfbc9fc74cde2^Ihttp://zhidao.baidu.com/question/414508398.html^Ihttp://www.baidu.com/s?word=%C9%ED%B7%DD%D6%A4%BA%C5%C2%EB%BA%CD%D5%E6%CA%B5%D0%D5%C3%FB%B2%E9%D1%AF&tn=utf8speed_dg&f=3^I1337932825
		String[] fields = line.split("\t");
		if (fields.length < 4) {
			logger.error("error! fileds<4, line=" + line);
			System.out.println("error! fileds<4, line=" + line + ".");
		} else {
			String user = fields[0];
			if (uidSet.size() > MAX_SIZE) {
				logger.warn("uidSet is too big! it may cause machine OOM, so do not add to uidSet, output it directly");
				output.collect(new Text(user), new IntWritable(1));
			} else {
				if (uidSet.add(user)) {
					output.collect(new Text(user), new IntWritable(1));
				} else {
					logger.debug(user + " already output, so donot output this time");
				}
			}
		}

	}
}