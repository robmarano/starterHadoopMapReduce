/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.cooper.ece465.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class hMR_patentCitation
	extends Configured implements Tool
{
	private String inputPath;
	private String outputPath;
	
	public static class TokenizerMapper
			extends Mapper<Text, Text, Text, Text>
	{
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException
		{
			System.out.println(key.toString()+","+value.toString());
			kt.set(key);
			vt.set(value);
			context.write(vt, kt);
		}
	}
	
	public static class PatentReducer
			extends Reducer<Text,Text,Text,Text>
	{
		private Text vt = new Text();
		private StringBuffer csv = new StringBuffer();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException
		{
			for (Text val:values)
			{
				if (csv.length() > 0)
				{
					csv.append(",");
				}
				csv.append(val.toString());
			}
			vt.set(csv.toString());
			context.write(key, vt);
			csv.delete(0, csv.length());
		}
	}
	
	@Override
	public int run(String[] args) throws Exception
	{
		Configuration conf = getConf();

		Map<String, String> env = System.getenv();
		Path coreSiteXml = new Path(env.get("HADOOP_CONF_DIR")+"/core-site.xml");
		Path hdfsSiteXml = new Path(env.get("HADOOP_CONF_DIR")+"/hdfs-site.xml");
		Path yarnSiteXml = new Path(env.get("HADOOP_CONF_DIR")+"/yarn-site.xml");
		Path mapredSiteXml = new Path(env.get("HADOOP_CONF_DIR")+"/mapred-site.xml");
		conf.addResource(coreSiteXml);
		conf.addResource(hdfsSiteXml);
		conf.addResource(yarnSiteXml);
		conf.addResource(mapredSiteXml);
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: hMR_patentCitation <in> <out>");
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}
		this.inputPath = otherArgs[0];
		this.outputPath = otherArgs[1];

		Job job = new Job(conf);
		job.setJarByClass(hMR_patentCitation.class);
		job.setJobName("Patent Citation");
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(PatentReducer.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path inputPath = new Path(this.inputPath);
		Path outputPath = new Path(this.outputPath);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		// execute job and return status
		boolean job_result = job.waitForCompletion(true);

		return((job_result) ? 0 : -1);
	}
	
	public static void main(String[] args)
		throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new hMR_patentCitation(), args);
		System.exit(res);
	}
}

