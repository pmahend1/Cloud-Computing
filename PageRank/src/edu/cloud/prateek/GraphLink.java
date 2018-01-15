/************************************************************************************************
 * File Name			: GraphLink.java
 * Author				: Prateek Mahendrakar
 * Description		: This program extracts contents of input files and creates graph link in the below format.
 * 							 <pageX> is text between <title> and <pageX_linkY> are outgoing links from that page 
 * 							  and *#*#*# is the delimiter I have used to separate links for later processing.
*************************************************************************************************/
package edu.cloud.prateek;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class GraphLink extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Log.info("Starting GraphLink job");
		System.out.println("Starting GraphLink job");
		int res = ToolRunner.run(new GraphLink(), args);
		System.exit(res);
	}

	public static class GraphLinkMap extends Mapper<LongWritable, Text, Text, Text> {

		// Regex pattern for finding out links in each line i.e. [[out-link]]
		private static final Pattern LINK_PATTERN = Pattern.compile("\\[\\[.*?]\\]");

		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
			String title;
			String outLink;
			String lineStr = line.toString();

			// return if empty line
			if (lineStr.isEmpty() || lineStr == null) {
				return;
			}

			// extract page title
			title = StringUtils.substringBetween(lineStr, "<title>", "</title>").trim();
			if (title.isEmpty() || title == null) {
				return;
			}

			Matcher matcher = LINK_PATTERN.matcher(lineStr);

			Log.info("getting outlinks for " + title);
			while (matcher.find()) {
				outLink = matcher.group().replace("[[", "").replace("]]", "");
				// outLink = outLink.trim();
				if (outLink.isEmpty()) {
					continue;
				}
				context.write(new Text(title), new Text(outLink));
			}

			// For the pages which do not have out links
			context.write(new Text(title), new Text(""));
		}

	}

	public static class GraphLinkReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text title, Iterable<Text> links, Context context) throws IOException, InterruptedException {

			ArrayList<String> outLinkList = new ArrayList<String>();

			// add out links into array
			for (Text link : links) {
				// For links that do not have out links
				if (link.toString().isEmpty()) {
					continue;
				}
				outLinkList.add(link.toString());
			}

			// output title and out-links with delimiter *#*#*#"
			context.write(title, new Text(StringUtils.join(outLinkList, "*#*#*#")));
		}

	}

	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("GraphLink");
		job.setMapperClass(GraphLinkMap.class);
		job.setReducerClass(GraphLinkReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
