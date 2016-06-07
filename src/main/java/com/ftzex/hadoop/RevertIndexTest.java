package com.ftzex.hadoop;

import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class RevertIndexTest {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private String filename;
		private Text documentId;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			documentId = new Text(filename);
		}
		
		// value已经是文件内容的一行,key是输入文件偏移量
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			for (String token:StringUtils.split(value.toString())){
				word.set(token);
				context.write(word, documentId);
			}
		}

		
		
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text docIds = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashSet<Text> uniqueDocIds = new HashSet<Text>();
			
			for (Text docId : values){
				//很奇怪的是，下面必须要创建一个副本，否则如果text1和text2同时出现的词，在最后结果中
				//两个文档的名字都是text2！待调查原因
				uniqueDocIds.add(new Text(docId));
			}
			docIds.set(new Text(StringUtils.join(uniqueDocIds, ",")));
			
			context.write(key, docIds);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: revertcount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "revert index");
//		Job job = new Job(conf, "word count");
		job.setJarByClass(RevertIndexTest.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		Path outputpath = new Path(otherArgs[1]);
		FileOutputFormat.setOutputPath(job, outputpath);
		//Delete original output dir before running the mr job
		outputpath.getFileSystem(conf).delete(outputpath,true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
