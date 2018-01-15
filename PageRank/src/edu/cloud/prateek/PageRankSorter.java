/*************************************************************************************
 * File Name			: PageRankSorter.java
 * Author				: Prateek Mahendrakar
 * Description		: This program sorts the pagerank results in descending order.
**************************************************************************************/
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;

public class PageRankSorter extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new PageRankSorter(), args);
		System.exit(res);
	}

	public static class PageRankSorterMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
			String title;
			String linkStr;
			String[] rankOut;
			String[] strArray;
			String pageRankStr;
			String lineStr = line.toString();

			// split to get title and page rank+out links
			strArray = lineStr.split("\t");
			title = strArray[0];
			linkStr = strArray[1];

			// Extracting page rank and out-links
			rankOut = linkStr.split("#####", 2);
			pageRankStr = rankOut[0];
			double pageRank = 0.0;
			try {
				pageRank = Double.parseDouble(pageRankStr);
			} catch (Exception e) {
				Log.info("Exception in parsing pageRankStr" + pageRankStr + " into double ");
			}

			// write out -1 x page rank , title
			// multiplied with -1 to get descending order
			context.write(new DoubleWritable(-1 * pageRank), new Text(title));
		}

	}

	public static class PageRankSorterReduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

		@Override
		public void reduce(DoubleWritable ranks, Iterable<Text> titles, Context context)
				throws IOException, InterruptedException {

			double rank = 0.0;

			// revert page rank to original value
			rank = ranks.get() * -1;

			// write out title and page ranks
			for (Text title : titles) {
				// t = title.toString();
				context.write(new Text(title.toString()), new DoubleWritable(rank));
			}

		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("PageRankSorter");
		job.setMapperClass(PageRankSorterMap.class);
		job.setReducerClass(PageRankSorterReduce.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		// Restricting reducer to 1
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
