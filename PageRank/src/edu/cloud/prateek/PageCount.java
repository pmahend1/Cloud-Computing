/*************************************************************************
 * File Name			: PageCount.java
 * Author				: Prateek Mahendrakar
 * Description		: This program iterates through input files and outputs the total number 
 * 							   of lines into final output.
*************************************************************************/
package edu.cloud.prateek;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.util.ToolRunner;

public class PageCount extends Configured implements Tool {
	public static void main(String[] args) throws Exception {

		System.out.println("*****************Running PageCount job*****************");
		System.out.println("Input path : " + args[0]);
		System.out.println("Output path : " + args[1]);

		int res = ToolRunner.run(new PageCount(), args);
		System.exit(res);
	}

	public static class PageCountMap extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
			// output 1 for each line
			if (line.toString().length() > 0) {
				context.write(new IntWritable(1), new IntWritable(1));
			}
		}
	}

	public static class PageCountReduce extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

		int count = 0;

		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			for (IntWritable i : values) {
				count++;
			}
			// write out the total count
			context.write(new Text("TotalPages"), new IntWritable(count));
		}
	}

	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("PageCount");
		job.setMapperClass(PageCountMap.class);
		job.setReducerClass(PageCountReduce.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
