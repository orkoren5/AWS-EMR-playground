import java.io.IOException;

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

import DataStructures.DSLayer3;
import DataStructures.DSLayer4;
import DataStructures.DataStructureBase;
import DataStructures.WordWordDecade;

public class Layer3 {

	public static class Layer3_Mapper extends Mapper<LongWritable, Text, WordWordDecade, DataStructureBase> {

		/**
		 * This mapper does nothing actually.. only passes the key-values as is to the reducer
		 */
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] keyValue = value.toString().split("\t");
			String str_wwdKey = keyValue[0];
			String str_value = keyValue[1];
			WordWordDecade wwdKey = null;
			DataStructureBase ds = null;
			
			try {
				wwdKey = WordWordDecade.parse(str_wwdKey);
				ds = DataStructureBase.parse(str_value);
			}
			catch( Exception ex) {
				System.out.println("EXCEPTION: " + ex);
				return;
			}

			System.out.println("Mapped: " + value);			
			context.write(wwdKey, ds);
		}

	}
	
	public static class Layer3_Reducer extends Reducer<WordWordDecade, DSLayer3, WordWordDecade, DataStructureBase> {
			
		private MultipleOutputs<WordWordDecade, DataStructureBase> mos;
		 
		public void setup(Context context) {
			System.out.println("--------------REDUCER SETUP-----------");
			 mos = new MultipleOutputs<WordWordDecade, DataStructureBase>(context);
		}
		
		public void reduce(WordWordDecade key, Iterable<DSLayer3> values, Context context)
				throws IOException, InterruptedException {
		
			System.out.println("Reducing: " + key.toString());
			long pairNum = 0, 
				word1Num = 0, 
				word2Num = 0; 
			
			// By design, values is has 2 members, or 1 if it is a decade only.
			for (DSLayer3 value : values) {	
				pairNum = value.getPairSum();
				if (word1Num == 0) {
					word1Num = value.getWordSum();
				} else {				
					word2Num = value.getWordSum();
				}
			}
			
			//mos.write("layer4", key, DataStructureBase.create(pairNum, word1Num, word2Num));
			context.write(key, DataStructureBase.create(pairNum, word1Num, word2Num));
		}
	}

	  public static void main(String[] args) throws Exception {
		  	System.out.println("RUNNING");	
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "ass2");
		    job.setJarByClass(Layer3.class);
		    job.setMapperClass(Layer3_Mapper.class);
		    job.setCombinerClass(Layer3_Reducer.class);
		    job.setReducerClass(Layer3_Reducer.class);
		    MultipleOutputs.addNamedOutput(job, "layer3", TextOutputFormat.class,
		    		 WordWordDecade.class, DSLayer3.class);
		    MultipleOutputs.addNamedOutput(job, "layer4", TextOutputFormat.class,
		    		 WordWordDecade.class, DSLayer4.class);
		    job.setOutputKeyClass(WordWordDecade.class);
		    job.setOutputValueClass(DSLayer3.class);
		    //job.setInputFormatClass(SequenceFileInputFormat.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}