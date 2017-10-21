/************************************************************************
 * File Name	:	DocWordCount.java
 * Author		:	Prateek Mahendrakar
 * Details		:	Outputs the word count for each distinct word
 *					in each file.Output will in the form 'word#####filename count'
 * 					where '#####' is the delimiter 
 ************************************************************************/

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class DocWordCount extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(DocWordCount.class);

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("Incorrect no of input parameters");
			System.out.println("Please enter below format");
			System.out.println("hadoop jar DocWordCount.jar edu.cloud.prateek.DocWordCount <input_path> <output_path>");
		}
		int res = ToolRunner.run(new DocWordCount(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " docWordCount ");
		job.setJarByClass(this.getClass());

		// Setting input and output paths
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Setting Mapper , Reducer and Output data type classes
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// return once job completes
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		// Intwritable writing count for each occurence
		private final static IntWritable IntWriteOne = new IntWritable(1);

		// Regex pattern to split words by whitespaces and linebreaks
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		// map method
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			LOG.info("Inside Map");
			// get input file name
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();

			String line = lineText.toString();

			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				// append ##### and filename to input word
				
				String newWord = word.toString() + "#####" + filename;
				LOG.info("newWord : "+newWord);
				
				Text wordOut = new Text(newWord);

				// write it out of mapper
				context.write(wordOut, IntWriteOne);
			}

		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			LOG.info("Inside Reduce");
			int sum = 0;

			// aggregate and sum up the counts
			for (IntWritable count : counts) {
				sum += count.get();
			}
			LOG.info("sum : "+sum);
			// final output
			context.write(word, new IntWritable(sum));
		}
	}
}
