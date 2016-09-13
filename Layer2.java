import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import DataStructures.DSLayer2;
import DataStructures.DSLayer3;
import DataStructures.DataStructureBase;
import DataStructures.WordWordDecade;


public class Layer2 {

	public static class Layer2_Mapper extends Mapper<LongWritable, Text, WordWordDecade, DataStructureBase> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("MAPPING: " + value);
			String[] keyValue = value.toString().split("\t");			
			String str_wwdKey = keyValue[0];
			String count = keyValue[1];			
			WordWordDecade wwdKey = null;
			
			try {
				wwdKey = WordWordDecade.parse(str_wwdKey);
			}
			catch( Exception ex) {
				System.out.println("EXCEPTION: " + ex);				
				System.out.println("EXCEPTION: " + ex.getStackTrace().toString());
				return;
			}
			
			DataStructureBase ds = DataStructureBase.create(wwdKey.getWord2(), Long.parseLong(count));
			wwdKey.clearWord2();
			System.out.println("Mapper Output: Key:" + wwdKey.toString() + ", Value " + ds.toString());
			context.write(wwdKey, ds);
		}

	}
	
	public static class Layer2_Reducer extends Reducer<WordWordDecade, DSLayer2, WordWordDecade, DataStructureBase> {
		
		/**
		 * The values which Layer2 reducer gets is designed to have only one value without a word, but only number.
		 * That number value is the sum of all appearances of word1 in a decade.
		 * @param values the list of values the reducer gets
		 * @return That value of the sum of all appearances of word1 in a decade.
		 */
		long getTheOnlyNumberValue(Iterable<DSLayer2> values, List<DSLayer2> cacheList) {
			long retVal = 0;
			for (DSLayer2 value : values) {								
				if (value.isWordEmpty()) {					
					retVal = value.getNumber();
				} else {
					cacheList.add(value);
				}
			}
			return retVal;
		}
		
		public void reduce(WordWordDecade key, Iterable<DSLayer2> values, Context context)
				throws IOException, InterruptedException {
		
			System.out.println("Reducing: " + key.toString());			
			List<DSLayer2> cacheList = new ArrayList<DSLayer2>();
			long totalNumberOfWord1 = getTheOnlyNumberValue(values, cacheList);			
			System.out.println("totalNumberOfWord1: " + String.valueOf(totalNumberOfWord1));			
			for (DSLayer2 value : cacheList) {				
				System.out.println("Value: " + value.toString());
				WordWordDecade new_wwdKey = new WordWordDecade(key.getWord1(), value.getWord(), key.getDecade());
				DataStructureBase new_value = DataStructureBase.create(value.getNumber(), totalNumberOfWord1);
				System.out.println("Writing - Key: " + new_wwdKey.toString() + ", Value: " + new_value.toString());
				context.write(new_wwdKey, new_value);
			}			
		}
	}

	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "ass2");
		    job.setJarByClass(Layer2.class);
		    job.setMapperClass(Layer2_Mapper.class);
		    job.setCombinerClass(Layer2_Reducer.class);
		    job.setReducerClass(Layer2_Reducer.class);
		    job.setOutputKeyClass(WordWordDecade.class);
		    job.setOutputValueClass(DataStructureBase.class);
		    //job.setInputFormatClass(SequenceFileInputFormat.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}