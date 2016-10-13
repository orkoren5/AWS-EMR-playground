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

import DataStructures.WordWordDecade;

public class Layer1AndHalf {

	public static class Layer1AndHalf_Mapper extends Mapper<LongWritable, Text, WordWordDecade, LongWritable> {
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//System.out.println("MAPPING L2: " + value);
			String[] keyValue = value.toString().split("\t");			
			long count = 0;			
			WordWordDecade wwdKey = null;
			
			try {
				wwdKey = WordWordDecade.parse(keyValue[0]);
				count = Long.parseLong(keyValue[1]);
			}
			catch( Exception ex) {
				System.out.println("EXCEPTION: " + ex);				
				System.out.println("EXCEPTION: " + ex.getStackTrace().toString());
				return;
			}
						
			context.write(wwdKey, new LongWritable(count));
			if (wwdKey.isCouple()) {
				context.write(new WordWordDecade(wwdKey.getWord1(), wwdKey.getDecade()), new LongWritable(count));
				context.write(new WordWordDecade(wwdKey.getWord2(), wwdKey.getDecade()), new LongWritable(count));
			}
		}

	}

	public static class Layer1AndHalf_Reducer extends Reducer<WordWordDecade, LongWritable, WordWordDecade, LongWritable> {
		
		public void reduce(WordWordDecade key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			
			// 'values' by design consists of only 1 member when the key is a couple
			if (key.isCouple()) {
				context.write(key, values.iterator().next());
				return;
			}
			
			long sumOfSingleWord = 0;
			for (LongWritable value : values) {					
				sumOfSingleWord += value.get();
			}		
			context.write(key, new LongWritable(sumOfSingleWord));
		}
	}

	  public static void main(String[] args) throws Exception {
		  	System.out.println("RUNNING L1.5");
			System.out.println("args[0]:" + args[0].toString() + "; args[1]:" + args[1].toString() + "; args[2]:" + args[2].toString());
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "ass2");
		    job.setJarByClass(Layer1AndHalf.class);
		    job.setMapperClass(Layer1AndHalf.Layer1AndHalf_Mapper.class);
		    job.setReducerClass(Layer1AndHalf.Layer1AndHalf_Reducer.class);
		    job.setOutputKeyClass(WordWordDecade.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[1]));
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}