/*********************************************************************
 * FileName	:	Search.java
 * Author	:	Prateek Mahendrakar
 * Details	:	Basic query search engine that takes user query and 
 * 				outputs list of documents that matches the query in 
 * 				the format 'filename TFIDFWeightSum' and input to
 * 				mapper is output of TFIDF.java
 *********************************************************************/

package edu.cloud.prateek;

import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Search extends Configured implements Tool {

	static int fileCount = 0;
	static Path inputPath;
	static Path tempPath;
	static Path outputPath;

	public static void main(String[] args) throws Exception {
		
		inputPath = new Path(args[0]);
		outputPath = new Path(args[1]);
		
		//get file count
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		ContentSummary cs = fs.getContentSummary(inputPath);
		fileCount = (int) cs.getFileCount();

		System.out.println("Input path : " + inputPath.toString());
		System.out.println("Output path : "+outputPath.toString());
		
		System.out.println("No of input files : " + fileCount);

		int res = ToolRunner.run(new Search(), args);

		System.exit(res);
	}

	public int run(String[] args) throws Exception {
	   
		//set search query terms in configuration for later use
		Configuration conf = getConf();
		int i = 0;
		for (String queryTerm : args) {
			if (queryTerm.equals(args[0]) || queryTerm.equals(args[1]))
				continue;
			conf.set("searchword" + i, queryTerm);
			
			Log.info("searchword" + i + " = " + queryTerm + " added");
			i++;
		}
		
		System.out.println("Execution at line 76") ;
		System.out.println("i/p path "+inputPath.toString());
		System.out.println("o/p path "+outputPath.toString());
		
		//set the total search terms too
		conf.set("searchWordCount", i + "");
		
		//start job
		Job job = Job.getInstance(getConf(), " hadoopsearch ");
		job.setJarByClass(this.getClass());

		//set input output paths
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		//set Mapper, Reducer and output data type classes
		job.setMapperClass(SearchMapper.class);
		job.setReducerClass(SearchReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	//Mapper class
	public static class SearchMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		//Line break is the separator 
		private static final Pattern WORD_BOUNDARY = Pattern.compile(System.lineSeparator());

		//map method
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			Log.info("Control is at SearchMapper");
			
			//set to save search strings and return
			HashSet<String> searchSet = new HashSet<String>();
			
			//get search word count
			Configuration conf = context.getConfiguration();
			String searchWordCountStr = conf.get("searchWordCount");
			int searchWordCount = 0;
			try {
				searchWordCount = Integer.parseInt(searchWordCountStr.trim());
			} catch (Exception e) {
				// TODO: handle exception
				Log.info("Control is at Exception");
				Log.info(e.getMessage());
				//System.exit(0);
			}
			Log.info("searchWordCount : " + searchWordCount); 
			
			//get all the search terms 
			for (int x = 0; x < searchWordCount; x++) {
				String str = conf.get("searchword" + x);
				searchSet.add(str);
				Log.info("searchword" + x + " value is retgrieved " + str);
			}

			String line = lineText.toString();

			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				
				//split by #####
				String[] lineSplits = word.toString().split("#####");
				Log.info("lineSplits are : " + lineSplits[0] + " " + lineSplits[1]);

				String searchWord = lineSplits[0];

				String[] secWordSplits = lineSplits[1].split("\\s+");
				Log.info("secWordSplits are : " + secWordSplits[0] + " " + secWordSplits[1]);

				String fileName = secWordSplits[0];
				String tdidfStr = secWordSplits[1];

				if (lineSplits.length > 0) {
					double tfIdf = 0.0;
					try {
						tfIdf = Double.parseDouble(tdidfStr.trim());
					} catch (Exception e) {
						// TODO: handle exception
						Log.info(e.getMessage());
					}
					
					//write out file name and tfidfs
					for (String str : searchSet) {
						if (str.equals(searchWord)) {
							context.write(new Text(fileName), new DoubleWritable(tfIdf));
						}
					}
				}

			}

		}

	}

	//Reducer 
	public static class SearchReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		
		//reduce method
		@Override
		public void reduce(Text word, Iterable<DoubleWritable> mappedTfIdfs, Context context)
				throws IOException, InterruptedException {
			
			//sum all tfidfs wrto document
			double sum = 0;
			Log.info("Control is at SearchReducer");
			for (DoubleWritable tfIdf : mappedTfIdfs) {
				sum += tfIdf.get();
			}
			Log.info("search frequency : "+sum);
			
			//write out final output
			context.write(word, new DoubleWritable(sum));
		}
	}

}