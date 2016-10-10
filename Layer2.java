import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
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
						
			context.write(wwdKey, DataStructureBase.create(wwdKey.getWord2(), count));
			if (wwdKey.isCouple()) {
				DataStructureBase ds2 = DataStructureBase.create(wwdKey.getWord1(), count);
				wwdKey.swap();
				//System.out.println("Mapper Output : Key:" + wwdKey.toString() + ", Value " + ds2.toString());
				context.write(wwdKey, ds2);
			} else {
				context.write(new WordWordDecade(wwdKey.getDecade()), DataStructureBase.create("", count));
			}
		}

	}
	
	public static class Layer2_GroupingComparator extends WritableComparator {
	    protected Layer2_GroupingComparator() {
	        super(WordWordDecade.class, true);
	        //System.out.println("Layer2_GroupingComparator");
	    }   
	    
	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	WordWordDecade k1 = (WordWordDecade)w1;
	    	WordWordDecade k2 = (WordWordDecade)w2;
	         	    	
			int res1 = Integer.compare(k1.getDecade(), k2.getDecade());
			int res2 = k1.getWord1().compareTo(k2.getWord1());				
			if(res1 != 0) {
				 //System.out.println("Layer2_GroupingComparator res1:" + res1);
				return res1;
			} else {
				//System.out.println("Layer2_GroupingComparator res2:" + res2);
				return res2;
			}	
	    }
	}
	
	
	

	  public static class PartitionerClass extends Partitioner<WordWordDecade, DataStructureBase>
		{
			@Override
			public int getPartition(WordWordDecade key, DataStructureBase value, int numPartitions)
			{
				int decade = key.getDecade(); 
				int decadeToPrint = decade % 12;
				//System.out.println("PartitionerClass L1 decadeToPrint:" + decadeToPrint);
				return decadeToPrint % numPartitions; //12 - num of decade from 1900 to 2020
				
			}
		}

	
	
	public static class Layer2_Reducer extends Reducer<WordWordDecade, DSLayer2, WordWordDecade, DataStructureBase> {
		
		public void reduce(WordWordDecade key, Iterable<DSLayer2> values, Context context)
				throws IOException, InterruptedException {
			
			// Case when the key is only a decade
			// Calculates the sum of all the single words to get the amount of all the words in the decade
			if (key.isDecadeOnly()) {
				long sumOfAllWordsInDecade = 0;
				for (DSLayer2 value : values) {	
					sumOfAllWordsInDecade += value.getNumber();
				}
				context.write(key, DataStructureBase.create(sumOfAllWordsInDecade, 0));
				return;
			}
			
			// Case when the is defined by the first word and decade
			Iterator<DSLayer2> it = values.iterator();			
			
			// The total number of word 1 will be the first value in the iterable
			// That's because we defined the secondary sort to be WordWordDecade's sort			
			long totalNumberOfWord1 = it.next().getNumber();		
			
			// If the key had only one value - then that key had only a decade in it (by design)
			if (!it.hasNext()) {
				WordWordDecade new_wwdKey = new WordWordDecade(key.getDecade());
				DataStructureBase new_value = DataStructureBase.create(totalNumberOfWord1, 0);
				//System.out.println("Writing - Key: " + new_wwdKey.toString() + ", Value: " + new_value.toString());
				context.write(new_wwdKey, new_value);
			} else {
				for (DSLayer2 value : values) {					
					WordWordDecade new_wwdKey = new WordWordDecade(key.getWord1(), value.getWord(), key.getDecade());
					DataStructureBase new_value = DataStructureBase.create(value.getNumber(), totalNumberOfWord1);
					//System.out.println("----Writing - Key: " + new_wwdKey.toString() + ", Value: " + new_value.toString());
					context.write(new_wwdKey, new_value);
				}		
			}
		}
	}

	  public static void main(String[] args) throws Exception {
		  	System.out.println("RUNNING L2");
			System.out.println("args[0]:" + args[0].toString() + "; args[1]:" + args[1].toString() + "; args[2]:" + args[2].toString());
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "ass2");
		    job.setJarByClass(Layer2.class);
		    job.setMapperClass(Layer2.Layer2_Mapper.class);
		    //job.setCombinerClass(Layer2.Layer2_Reducer.class);
		    job.setGroupingComparatorClass(Layer2.Layer2_GroupingComparator.class);
		    job.setPartitionerClass(PartitionerClass.class);
		    job.setReducerClass(Layer2.Layer2_Reducer.class);
		    job.setMapOutputKeyClass(WordWordDecade.class);
		    job.setMapOutputValueClass(DSLayer2.class);
		    job.setOutputKeyClass(WordWordDecade.class);
		    job.setOutputValueClass(DSLayer3.class);
		    //job.setInputFormatClass(SequenceFileInputFormat.class);
		    FileInputFormat.addInputPath(job, new Path(args[1]));
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}