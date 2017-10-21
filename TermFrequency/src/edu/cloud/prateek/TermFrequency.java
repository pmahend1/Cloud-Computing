/**************************************************************************
 * FileName	:	TermFrequency.java
 * Author	:	Prateek Mahendrakar
 * Details	:	Outputs term frequency(TF) for each word in the corpus in the 
 * 				format 'word#####filename TF' where ##### is delimiter
 ***************************************************************************/

package edu.cloud.prateek;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TermFrequency extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TermFrequency.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TermFrequency(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " wordcount ");
		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	//mapper
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		//IntWritable for count output
		private final static IntWritable IntWriteOne = new IntWritable(1);
		
		//regex to split words by space and line breaks
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		//map method
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();

			String line = lineText.toString();

			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				String newWord = word.toString() + "#####" + filename;
				Text wordOut = new Text(newWord);
				
				//output count
				context.write(wordOut, IntWriteOne);
			}

		}
	}

	//reducer
	public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			//term frequency
			double wf = 1+ Math.log10(sum);
			
			context.write(word, new DoubleWritable(wf));
		}
	}
}
