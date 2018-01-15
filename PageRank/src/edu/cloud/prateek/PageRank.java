/*************************************************************************
 * File Name			: PageRank.java
 * Author				: Prateek Mahendrakar
 * Description		: This program calculates page rank of the pages.
 * 							  with Damping factor = 0.85
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;

public class PageRank extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println("Running PageRank job");
		System.out.println("Input path " + args[0]);
		System.out.println("Output path " + args[1]);

		int res = ToolRunner.run(new PageRank(), args);
		System.exit(res);
	}

	public static class PageRankMap extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
			String title;
			String lineStr;
			String outLinksStr;
			String[] outLinksArray = null;
			String[] tempArray;
			double pageRank = 0;
			double weightage;
			String[] lineStrArray;

			lineStr = line.toString();

			// split title and pagerank+out-links
			Log.info("At execution of line = " + lineStr);
			lineStrArray = lineStr.split("\t");

			title = lineStrArray[0];
			outLinksStr = lineStrArray[1];

			Log.info("After split by tab " + title + "\t" + outLinksStr);

			// get pagerank and outlinks in different variables
			tempArray = outLinksStr.split("#####", 2);
			if (tempArray.length > 1) {
				Log.info("Splitting second part by hashes : " + tempArray[0] + "	" + tempArray[1]);
			}

			// parse pagerank string into double
			try {
				pageRank = Double.parseDouble(tempArray[0]);
			} catch (Exception e) {
				Log.info("Error in parsing string to double for page rank " + e.getMessage());
			}

			// if out-links exists split and add the links
			if (tempArray.length > 1 && !tempArray[1].isEmpty()) {
				Log.info("trimming out *#*#*#  : ");
				outLinksArray = tempArray[1].split("\\*#\\*#\\*#");
				Log.info("After splitting by *#*#*# " + outLinksArray.toString());
			}

			context.write(new Text(title), new Text("+++"));

			// Dividing page rank by no. of out-links
			if ((tempArray.length > 1 && !tempArray[1].isEmpty())) {
				weightage = pageRank / outLinksArray.length;

				for (String outlink : outLinksArray) {
					// write out out-link and weightage
					context.write(new Text(outlink), new Text(Double.toString(weightage)));
				}
			}

			// Write out title and outgoing links : "##*##" as a delimiter
			if (tempArray.length > 1 && !tempArray[1].isEmpty()) {
				context.write(new Text(title), new Text("##*##" + tempArray[1]));
			} else {
				context.write(new Text(title), new Text("##*##"));
			}
		}

	}

	public static class PageRankReduce extends Reducer<Text, Text, Text, Text> {

		// damping factor
		private double DF = 0.85;

		@Override
		public void reduce(Text title, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double contributions = 0.0;
			double pageRank;
			boolean isPage = false;
			String outLinks = "";
			String v;

			// Iterate list of values
			for (Text val : values) {
				Log.info("val value is" + val.toString());
				v = val.toString().trim();

				Log.info("v is :" + v);

				if (v.equals("+++")) {
					isPage = true;
					continue;
				}

				try {
					// Will throw an exception if v is not a number
					contributions += Double.parseDouble(v);
				} catch (NumberFormatException e) {
					// Getting the out-links
					outLinks = v.split("##\\*##", 2)[1];
				}
			}

			// If not wiki page exit
			if (!isPage) {
				return;
			}

			// Compute new page rank
			pageRank = (1.0 - DF) + (DF * contributions);

			// write out title, new page rank , out-links
			context.write(new Text(title), new Text(Double.toString(pageRank) + "#####" + outLinks));
		}

	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("PageRank");
		job.setMapperClass(PageRankMap.class);
		job.setReducerClass(PageRankReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
