/*******************************************************************************************
 * File Name			: Driver.java
 * Author				: Prateek Mahendrakar
 * Description		: This runs all the above programs in the following order
 * 								i) GraphLink
 *								ii) PageCount
 *								iii) PageRankInitializer
 *								iv) PageRank x 10 times
 *								v) PageRankSorter
 *					  		 Deletes PageRank intermediate paths after successful completion of jobs.
***********************************************************************************************/
package edu.cloud.prateek;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class Driver {

	// method to delete intermediate directories
	public void deletePath(Path path) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(path, true);
	}

	public static void main(String[] args) throws Exception {

		System.out.println("********************Starting Execution*****************************");
		// path variables and initialization
		String inputPath = args[0];
		String outputPath = args[1];

		// setting up output directories for individual jobs
		String graphLinkPath = outputPath + "/GraphLink";
		String pageCountPath = outputPath + "/PageCount";
		String pageRankInitialPath = outputPath + "/PageRankInitializer";
		String pageRankPath = outputPath + "/PageRank";
		String pageRankFinalOutputPath = "";
		String finalOutputPath = outputPath + "/FinalOutPut";

		int runCount = 0;

		Driver pageRank = new Driver();

		String inIterate;
		String outIterate;

		// Run GraphLink
		System.out.println("Submitting GraphLink job");
		int status = ToolRunner.run(new GraphLink(), new String[] { inputPath, graphLinkPath });

		// Run PageCount
		System.out.println("Submitting PageCount job");
		status = ToolRunner.run(new PageCount(), new String[] { inputPath, pageCountPath });

		// Run PageRankInitializer
		System.out.println("Submitting PageRankInitializer job");
		status = ToolRunner.run(new PageRankInitializer(),
				new String[] { graphLinkPath, pageCountPath, pageRankInitialPath });

		int i = 0;
		// Driver calculation for 10 iterations
		for (i = 1; i <= 10; i++) {
			if (i == 1) {
				// For the first iteration using pageRankInitializer
				inIterate = pageRankInitialPath;
			} else {
				inIterate = pageRankPath + "/Iteration" + Integer.toString(i - 1);
			}
			outIterate = pageRankPath + "/Iteration" + Integer.toString(i);

			System.out.println("Submitting PageRank job | iteration " + i);
			status = ToolRunner.run(new PageRank(), new String[] { inIterate, outIterate });

			// Cleaning the intermediate iteration outputs after successful
			// completion of the job
			if (status == 0) {
				try {
					pageRank.deletePath(new Path(inIterate));
					System.out.println("Path :" + inIterate + " deleted");
				} catch (Exception e) {
					System.out.println("Unable to delete path:" + inIterate + "\t" + e.getMessage());
				}

			}

		}

		// i is 11
		runCount = i - 1;

		// final output path from PageRank
		pageRankFinalOutputPath = pageRankPath + "/Iteration" + Integer.toString(runCount);

		// Run PageRankSorter job
		System.out.println("Submitting PageRankSorter job");
		status = ToolRunner.run(new PageRankSorter(), new String[] { pageRankFinalOutputPath, finalOutputPath });

		if (status == 0) {
			System.out.println("PageRanking successfully completed");
			System.out.println("Graph link output is at : " + graphLinkPath);
			System.out.println("PageCount output is at : " + pageCountPath);
			System.out.println("Final output is at " + finalOutputPath);
		} else {
			System.out.println("PageRanking did not finish correctly. ");
		}
	}
}
