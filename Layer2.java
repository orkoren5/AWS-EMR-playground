import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import DataStructures.DSLayer2;
import DataStructures.DSLayer3;
import DataStructures.DataStructureBase;
import DataStructures.WordWordDecade;


public class Layer2 {

	public static class Layer2_Mapper extends Mapper<LongWritable, Text, WordWordDecade, DataStructureBase> {
		
		private MultipleOutputs<WordWordDecade, DataStructureBase> mos;
		 
		public void setup(Context context) {
			System.out.println("--------------MAPPER SETUP-----------&&&&&");
			 mos = new MultipleOutputs<WordWordDecade, DataStructureBase>(context);
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
				System.out.println("--------------MAPPER CLEANUP-----------");
				mos.close();
		}
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("MAPPING: " + value);
			String[] keyValue = value.toString().split("\t");
			String str_wwdKey = keyValue[0];
			String count = keyValue[1];			
			WordWordDecade wwdKey = null;
			WordWordDecade wwdKeyCopy = null;
			
			try {
				wwdKey = WordWordDecade.parse(str_wwdKey);
				wwdKeyCopy = WordWordDecade.copy(wwdKey);
			}
			catch( Exception ex) {
				System.out.println("EXCEPTION: " + ex);				
				System.out.println("EXCEPTION: " + ex.getStackTrace().toString());
				return;
			}
			
			DataStructureBase ds1 = DataStructureBase.create(wwdKey.getWord2(), Long.parseLong(count));
			wwdKey.clearWord2();
			System.out.println("Mapper Output: Key:" + wwdKey.toString() + ", Value " + ds1.toString());			
			context.write(wwdKey, ds1);
			//mos.write("layer2", wwdKey, ds1);
			
			if (wwdKeyCopy.isCouple()) {
				DataStructureBase ds2 = DataStructureBase.create(wwdKeyCopy.getWord1(), Long.parseLong(count));
				wwdKeyCopy.clearWord1();
				System.out.println("Mapper Output: Key:" + wwdKeyCopy.toString() + ", Value " + ds2.toString());
				context.write(wwdKeyCopy, ds2);
				//mos.write("layer2", wwdKey2, ds2);
			}
		}

	}
	
	public static class Layer2_Reducer extends Reducer<WordWordDecade, DSLayer2, WordWordDecade, DataStructureBase> {
		
		private MultipleOutputs<WordWordDecade, DataStructureBase> mos;
		 
		public void setup(Context context) {
			System.out.println("--------------REDUCER SETUP-----------");
			 mos = new MultipleOutputs<WordWordDecade, DataStructureBase>(context);
		}
		
		/**
		 * The values which the Reducer gets is designed to consist of only one value with an empty word.
		 * That number value is the sum of all appearances of word1 in a decade.
		 * @param values the list of values the reducer gets
		 * @return That value of the sum of all appearances of word1 in a decade.
		 */
		long getTheOnlyNumberValue(Iterable<DSLayer2> values, List<DSLayer2> cacheList) {
			long retVal = 0;
			for (DSLayer2 value : values) {	
				System.out.println("---- " + value.toString() + ", is Empty: " + String.valueOf(value.isWordEmpty()));
				if (value.isWordEmpty()) {					
					retVal = value.getNumber();
				} else {
					cacheList.add((DSLayer2)value.copy());
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
				WordWordDecade new_wwdKey = new WordWordDecade(key.getWord1(), value.getWord(), key.getDecade());
				DataStructureBase new_value = DataStructureBase.create(totalNumberOfWord1, value.getNumber());
				System.out.println("Writing - Key: " + new_wwdKey.toString() + ", Value: " + new_value.toString());
				mos.write("layer3", new_wwdKey, new_value);
			}	
			
			// If the key had only one value - then that key had only a decade in it (by design)
			if (cacheList.isEmpty()) {
				WordWordDecade new_wwdKey = new WordWordDecade(key.getDecade());
				DataStructureBase new_value = DataStructureBase.create(totalNumberOfWord1, 0);
				System.out.println("Writing - Key: " + new_wwdKey.toString() + ", Value: " + new_value.toString());
				mos.write("layer3", new_wwdKey, new_value);
			}
		}
	}

	  public static void main(String[] args) throws Exception {
		  	System.out.println("RUNNING");	
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "ass2");
		    job.setJarByClass(Layer2.class);
		    job.setMapperClass(Layer2_Mapper.class);
		    job.setCombinerClass(Layer2_Reducer.class);
		    job.setReducerClass(Layer2_Reducer.class);
		    MultipleOutputs.addNamedOutput(job, "layer2", TextOutputFormat.class,
		    		 WordWordDecade.class, DSLayer2.class);
		    MultipleOutputs.addNamedOutput(job, "layer3", TextOutputFormat.class,
		    		 WordWordDecade.class, DSLayer3.class);
		    job.setOutputKeyClass(WordWordDecade.class);
		    job.setOutputValueClass(DSLayer2.class);
		    //job.setInputFormatClass(SequenceFileInputFormat.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}