/*********************************************************************************
 * FileName	:	TFIDF.java
 * Author	:	Prateek Mahendrakar
 * Details	:	Calculates Term Frequency for each word in corpus(TF) and Inverse 
 * 				Document Frequency(IDF) for each word and then outputs TF-IDF in
 * 				the format 'word#####filename TFIDF' where ##### is delimiter
 *********************************************************************************/

package edu.cloud.prateek;

import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import edu.cloud.prateek.TFIDF.TFIDFMapper.IDFReducer;
import edu.cloud.prateek.TFIDF.TFIDFMapper.TFReducer;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);

	static int fileCount = 0;
	static Path inputPath;
	static Path tempPath;
	static Path outputPath;

	public static void main(String[] args) throws Exception {

		// create i/p o/p path
		inputPath = new Path(args[0]);
		tempPath = new Path(args[0] + "/temp"); // input/temp used as
												// intermediate path between
												// map1 map2 phases
		outputPath = new Path(args[1]);

		// Get the input file count
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		ContentSummary cs = fs.getContentSummary(inputPath);
		fileCount = (int) cs.getFileCount();

		System.out.println("No of input files : " + fileCount);

		int res = ToolRunner.run(new TFIDF(), args);

		System.exit(res);
	}

	// run method of execution
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), " tfidf ");
		job.setJarByClass(this.getClass());

		// set job input output paths
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, tempPath);

		// set mapper, reducer and output data type classes
		job.setMapperClass(TFMapper.class);
		job.setReducerClass(TFReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// wait for first mapper job to finish
		int status = job.waitForCompletion(true) ? 0 : 1;

		// Mapper 2
		// once mapper 1 is complete proceed with mapper 2 execution
		if (status == 0) {

			System.out.println("status " + status);

			// set fileCount in order to access later in tf-idf computation in
			// reducer phase
			Configuration conf = getConf();
			conf.set("filecount", fileCount + "");
			Job job2 = Job.getInstance(getConf(), " wordcount ");

			job2.setJarByClass(this.getClass());

			// logging to verify path values
			System.out.println("Input path :" + tempPath.toString());
			System.out.println("Output path :" + outputPath.toString());

			// set input and output paths for job
			FileInputFormat.addInputPath(job2, tempPath);
			FileOutputFormat.setOutputPath(job2, outputPath);

			// set Mapper, Reducer and Output data type classes
			job2.setMapperClass(TFIDFMapper.class);
			job2.setReducerClass(IDFReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			// wait for job to finish and return
			return job2.waitForCompletion(true) ? 0 : 1;

		} else {
			System.out.println("Error in first map job . Quitting execution! Review log for more details.");
			return 1;
		}
	}

	// First Mapper class to compute Word Frequency
	public static class TFMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable IntWriteOne = new IntWritable(1);

		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

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
				context.write(wordOut, IntWriteOne);
			}
		}
	}

	// Second mapper class
	public static class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {

		// setting new line as line separator regex
		private static final Pattern WORD_BOUNDARY = Pattern.compile(System.lineSeparator());

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String line = lineText.toString();

			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}

				// split words by #####
				String[] s = word.split("#####");

				LOG.info("String s : " + s.toString());
				System.out.println("String s : " + s.toString());

				String s0 = s[0];

				Log.info("s0 " + s0);
				String s1 = s[1];

				// trim and replace whitespace with =
				System.out.println("s1 before : " + s1);
				Log.info("s1 before : " + s1);
				s1 = s1.replace("\t", "=");
				s1 = s1.replaceAll("\\s+", "=");

				System.out.println("s1 after: " + s1);
				Log.info("s1 after: " + s1);

				// create Text obj
				Text t2 = new Text(s1);
				Text t1 = new Text(s0);

				// write out mapper outputs
				context.write(t1, t2);
			}

		}

		// Reducer 1 for TFMapper
		public static class TFReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
			@Override
			public void reduce(Text word, Iterable<IntWritable> counts, Context context)
					throws IOException, InterruptedException {
				int sum = 0;
				for (IntWritable count : counts) {
					sum += count.get();
				}

				double wf = 0.0;
				// word frequency calculation
				if (sum > 0) {
					wf = 1 + Math.log10(sum);
				}

				// write out <word,wordfrequncy>
				context.write(word, new DoubleWritable(wf));
			}
		}

		// Reducer 2 for IDFMapper
		public static class IDFReducer extends Reducer<Text, Text, Text, DoubleWritable> {

			// HashMap for saving word frequencies for later use
			HashMap<String, Double> fileNameMap = new HashMap<String, Double>();

			// reduce method
			@Override
			public void reduce(Text word, Iterable<Text> counts, Context context)
					throws IOException, InterruptedException {

				// counter to track no. of per document occurences
				int i = 0;

				// clear the hashmap for each word
				fileNameMap.clear();

				// get input total no. of documents
				Configuration conf = context.getConfiguration();
				String filecountStr = conf.get("filecount");

				// trim
				filecountStr = filecountStr.trim();
				int filecount = 0;
				try {
					filecount = Integer.parseInt(filecountStr);
				} catch (Exception e) {
					// TODO: handle exception
					Log.info(e.getMessage());
				}

				String[] wfArr;
				for (Text count : counts) {
					Log.info("reducer iterable " + count.toString());
					i = i + 1;

					Log.info("Incrementing i" + i);

					// split by = character
					wfArr = count.toString().split("=", 2);
					Log.info("wf values " + wfArr[0] + " and " + wfArr[1]);

					// extract word frequency
					double wf = 0.0;
					try {
						wf = Double.parseDouble(wfArr[1]);
					} catch (Exception e) {
						Log.warn("exception in parse double ");
					}

					// save word frequency in map
					fileNameMap.put(wfArr[0], wf);
				}
				double idf = 0.0;

				// calculate IDF
				if (i > 0) {
					Log.info("filecount in IDF Reducer " + filecount);
					idf = Math.log10(1 + (filecount / i));
					Log.info("idf value :" + idf + "");
				}

				// write out TF-IDFs
				for (String key : fileNameMap.keySet()) {
					String outword = word.toString() + "#####" + key;
					double tfIdf = fileNameMap.get(key) * idf;
					context.write(new Text(outword), new DoubleWritable(tfIdf));
				}

			}
		}
	}
}