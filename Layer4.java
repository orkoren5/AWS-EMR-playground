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

import DataStructures.DSLayer5;
import DataStructures.DSLayer6;
import DataStructures.DataStructureBase;
import DataStructures.WordWordDecade;


public class Layer4 
{
	public static class Layer4_Mapper extends Mapper<LongWritable, Text, WordWordDecade, DataStructureBase> {
		
	/*	private MultipleOutputs<WordWordDecade, DataStructureBase> mos;
		 
		public void setup(Context context) {
			System.out.println("--------------MAPPER SETUP-----------&&&&&");
			 mos = new MultipleOutputs<WordWordDecade, DataStructureBase>(context);
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
				System.out.println("--------------MAPPER CLEANUP-----------");
				mos.close();
		}*/
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("MAPPING: " + value);
			
			String[] keyValue = value.toString().split("\t");
			String keys = keyValue[0];
			String Values = keyValue[1];
			
			
			String[] sValue = Values.toString().split(" ");
			String PairCount = sValue[0];
			String Word1Count = sValue[1];
			String Word2Count = sValue[2];
				
			WordWordDecade wwdKey = null;
			
			try {
				wwdKey = WordWordDecade.parse(keys);
			}
			catch( Exception ex) {
				System.out.println("EXCEPTION: " + ex);				
				System.out.println("EXCEPTION: " + ex.getStackTrace().toString());
				return;
			}
			DataStructureBase ds5 = DataStructureBase.create(wwdKey.getWord1(),wwdKey.getWord2(),Long.parseLong(PairCount), Long.parseLong(Word1Count), Long.parseLong(Word2Count));
			System.out.println("Mapper Output: Key:" + key.toString() + ", Value " + ds5.toString());			
			wwdKey.clearWord1();
			wwdKey.clearWord2();
			context.write(wwdKey, ds5);		
		}
	}
	
	public static class Layer4_Reducer extends Reducer<WordWordDecade, DSLayer5, WordWordDecade, DSLayer6> {
		
		/*private MultipleOutputs<WordWordDecade, DataStructureBase> mos;
		 
		public void setup(Context context) {
			System.out.println("--------------REDUCER SETUP-----------");
			 mos = new MultipleOutputs<WordWordDecade, DataStructureBase>(context);
		}*/
		
		/**
		 * The values which the Reducer gets is designed to consist of only one value with an empty word.
		 * That number value is the sum of all appearances of word1 in a decade.
		 * @param values the list of values the reducer gets
		 * @return That value of the sum of all appearances of word1 in a decade.
		 */
		long getTheOnlyNumberValue(Iterable<DSLayer5> values, List<DSLayer5> cacheList) {
			long retVal = 0;
			for (DSLayer5 value : values) {	
				System.out.println("---- " + value.toString() + ", is Empty: " + String.valueOf(value.isWord1Empty()));
				if (value.isWord1Empty()) {					
					retVal = value.getNum3();
				} else {
					cacheList.add((DSLayer5)value.copy());
				}
			}
			return retVal;
		}
		
		public void reduce(WordWordDecade key, Iterable<DSLayer5> values, Context context)
				throws IOException, InterruptedException {
		
			System.out.println("Reducing: " + key.toString());		
			
			List<DSLayer5> cacheList = new ArrayList<DSLayer5>();
			
			long totalNumberInDecade = getTheOnlyNumberValue(values, cacheList);		
			
			System.out.println("totalNumberInDecade: " + String.valueOf(totalNumberInDecade));			
			for (DSLayer5 value : cacheList) {				
				WordWordDecade new_wwdKey = new WordWordDecade(value.getWord1(), value.getWord2(), key.getDecade());
				double pmiCalc = Math.log((value.getNum1()*totalNumberInDecade)/(value.getNum2()*value.getNum3()));
				//DoubleWritable pmi = new DoubleWritable(pmiCalc);
				DataStructureBase new_value = DataStructureBase.create(value.getNum1(), value.getNum2(), value.getNum3(),totalNumberInDecade, pmiCalc);
				System.out.println("Writing - Key: " + new_wwdKey.toString() + ", Value: " + new_value.toString());
				context.write(new_wwdKey, new_value);
				//mos.write("layer3", new_wwdKey, new_value);
			}	
			
			// If the key had only one value - then that key had only a decade in it (by design)
			if (cacheList.isEmpty()) {
				System.out.println("oops - cacheList isEmpty");
				//WordWordDecade new_wwdKey = new WordWordDecade(key.getDecade());
				//DataStructureBase new_value = DataStructureBase.create(totalNumberOfWord1, 0);
				//System.out.println("Writing - Key: " + new_wwdKey.toString() + ", Value: " + new_value.toString());
				//mos.write("layer3", new_wwdKey, new_value);
			}
		}
	}

	  public static void main(String[] args) throws Exception {
		  	System.out.println("RUNNING");	
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "ass2");
		    job.setJarByClass(Layer4.class);
		    job.setMapperClass(Layer4_Mapper.class);
		    job.setCombinerClass(Layer4_Reducer.class);
		    job.setReducerClass(Layer4_Reducer.class);
		    job.setOutputKeyClass(WordWordDecade.class);
		    job.setOutputValueClass(DSLayer6.class);
		    //job.setInputFormatClass(SequenceFileInputFormat.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}