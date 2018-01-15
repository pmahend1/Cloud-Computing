/************************************************************************************************
 * File Name			: PageRankInitializer.java
 * Author				: Prateek Mahendrakar
 * Description		: Initializes PageRank by taking initial page rank = 1/N where N is Total no. of pages 
 * 							  Output will be in the form of 
 * 							  <page1>	<XXXXXX>#####<page1_link1>*#*#*#<page1_link2>....
**************************************************************************************************/
package edu.cloud.prateek;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;

public class PageRankInitializer extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		Log.info("Starting PageRankInitializer job");
		System.out.println("Starting PageRankInitializer job");
		int res = ToolRunner.run(new PageRankInitializer(), args);
		System.exit(res);
	}

	public static class PageRankInitializerMap extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
			String title;
			String outLinksStr = "";
			String[] lineStrArray;

			String lineStr = line.toString();

			// split by tab
			lineStrArray = lineStr.split("\\t");

			// get title and out links
			title = lineStrArray[0];

			// only if links exists
			if (lineStrArray.length == 2) {
				outLinksStr = lineStrArray[1];
			}

			// write out title and out-links
			context.write(new Text(title), new Text(outLinksStr.trim()));
		}

	}

	public static class PageRankInitializerReducer extends Reducer<Text, Text, Text, Text> {

		private int pageCount;

		// Setting up the page count from configuration.
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			pageCount = conf.getInt("PAGECOUNT", -1);
		}

		@Override
		public void reduce(Text title, Iterable<Text> links, Context context) throws IOException, InterruptedException {
			double initialPageRank;
			String outlinks;

			if (pageCount != -1) {

				// Initial page rank, 1.0/Total pages
				initialPageRank = 1.0 / pageCount;

				// write out title, initial page rank and out-links
				for (Text link : links) {
					outlinks = link.toString().trim();
					context.write(new Text(title), new Text(initialPageRank + "#####" + outlinks));
				}
			} else {
				Log.warn("Error in finding and setting page count. Review PageCount job!");
				return;
			}

		}

	}

	public int run(String[] args) throws Exception {
		int pageCount = getPageCountFromFS(args[1]);

		Configuration conf = new Configuration();
		conf.setInt("PAGECOUNT", pageCount);

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("PageRankInitializer");
		job.setMapperClass(PageRankInitializerMap.class);
		job.setReducerClass(PageRankInitializerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	// Reading the file to get the count from PageCount
	private int getPageCountFromFS(String pathStr) throws IOException {
		FileSystem fs = null;
		String[] strArray = new String[2];
		BufferedReader br = null;
		String line = null;
		Path path;

		try {
			path = new Path(pathStr);

			fs = path.getFileSystem(new Configuration());

			FileStatus[] fileStat = fs.listStatus(path);
			Path[] allFiles = FileUtil.stat2Paths(fileStat);

			for (Path file : allFiles) {
				if (fs.isFile(file)) {
					br = new BufferedReader(new InputStreamReader(fs.open(file)));
					line = br.readLine();

					if (line != null && !line.isEmpty()) {
						if (line.contains("\t")) {
							strArray[1] = line.split("\t")[1];
							break;
						}
					}
				}
			}
		} catch (IOException e) {
			Log.warn("IOException " + e.getMessage());
		} finally {
			try {
				br.close();
				fs.close();
			} catch (IOException e) {
				Log.warn("IOException " + e.getMessage());
			}
		}
		return Integer.parseInt(strArray[1]);
	}
}
