import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Student ID: 2646502
 *
 */

public class MovieReview 
{

	// The mapper class
	public static class RatingMapper extends Mapper<Object, Text, IntWritable, Text>
	{
		ArrayList<String> exWords = new ArrayList<>();

		public void setup(Context context) {
			try
			{
				String fn = context.getCacheFiles()[0].toString();
				BufferedReader in = new BufferedReader(new FileReader(fn));
				String line = in.readLine();
				StringTokenizer st = new StringTokenizer(line, ",");
				while (st.hasMoreTokens()) {
					exWords.add(st.nextToken());
				}
				in.close();
			}
			catch (Exception e)
			{
				System.err.println("The file does not exist " + e);
			}
		}
	    /**
	     * Processes a line that is passed to it by writing a key/value pair to the context. 
	     * 
	     * @param index 	A link to the file index
	     * @param value 	The line from the file
	     * @param context 	The Hadoop context object
	     */
	    public void map(Object index, Text value, Context context) throws IOException, InterruptedException 
	    {

			ArrayList<String> accWords = new ArrayList<>();
	    	StringTokenizer itr = new StringTokenizer(value.toString(), "\r");	// Tokenizer for line from file

	    	while (itr.hasMoreTokens())
	    	{
				String[] review = itr.nextToken().split("\t");
				String reviewNoQuotes = review[0].replace("\"", "");
				String[] words = reviewNoQuotes.split(" ");
				int score = Integer.parseInt(review[1].trim());

				for(int i = 0; i < words.length; i++) {
					if (!exWords.contains(words[i]) && !words[i].equals(",") && !words[i].equals("")) {
						accWords.add(words[i]);
						context.write(new IntWritable(score), new Text(words[i]));
					}
				}


	    		// Write, i.e. emit word as key and 1 as value (IntWritable(1))


	    	}


	    }
	    

//	    
//	    public void cleanup(Context context)
//	    {
//	    	System.out.println("Cleaning up mapper");
//	    }
	}



	// The Reducer class
	public static class RatingReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
		/**
		 * Reduces multiple data values for a given key into a single output for that key
		 *
		 * @param key     The key that this particular reduce call will be operating on
		 * @param values  An array of values associated with the given key
		 * @param context The Hadoop context object
		 */
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			ArrayList<String> words = new ArrayList<>();
			HashMap<String, Integer> wordCounts = new HashMap<>();


				for (Text x : values) {
					words.add(x.toString());
				}

				for(int i=0; i<words.size(); i++) {

					String word = words.get(i);
					int count = Collections.frequency(words, word);
					wordCounts.put(word, count);
				}

				System.out.println(wordCounts.values());

				int maxValueInMap = (Collections.max(wordCounts.values()));  // This will return max value in the Hashmap

				for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {  // Iterate through hashmap

					if (entry.getValue() == maxValueInMap) {

						//System.out.println(entry.getKey());
						String common = entry.getKey();
						Text mostCommon = new Text(common);
						context.write(key, mostCommon);

					}
				}




			// Iterate through the values for the given key and sum them (essentially add
			// all the 1s, so count, except that it might be called more than once (or combined),
			// so must be sum, not ++1)


			// Set value of result to sum
			//IntWritable result = new IntWritable(sum);
			// Emit key and result (i.e word and count).
		}

//	    public void setup(Context context)
//	    {
//	    	System.out.println("Setting up reducer");
//	    }
//	    
//	    public void cleanup(Context context)
//	    {
//	    	System.out.println("Cleaning up reducer");
//	    }

	}

	  /**
	   * main program that will be run, including configuration setup
	   * 
	   * @param args		Command line arguments
	   */
	  public static void main(String[] args) throws Exception 
	  {		
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Movie Rating Words");
	    job.setJarByClass(MovieReview.class);
	    
	    // Set mapper class to TokenizerMapper defined above
	    job.setMapperClass(RatingMapper.class);
	    
	    // Set combine class to IntSumReducer defined above
	    //job.setCombinerClass(RatingReducer.class);
	    
	    // Set reduce class to IntSumReducer defined above
	    job.setReducerClass(RatingReducer.class);
	    
	    // Class of output key is IntWritable
	    job.setOutputKeyClass(IntWritable.class);
	    
	    // Class of output value is Text
	    job.setOutputValueClass(Text.class);
	    
	    // Input path is first argument when program called
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    
	    // Output path is second argument when program called
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));	
	    
	    // waitForCompletion submits the job and waits for it to complete, 
	    // parameter is verbose. Returns true if job succeeds.
	    System.exit(job.waitForCompletion(true) ? 0 : 1);		
	  }
}



