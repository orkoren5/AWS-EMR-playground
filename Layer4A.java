import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import DataStructures.DSLayer4;
import DataStructures.DSLayer5;
import DataStructures.DataStructureBase;
import DataStructures.WordWordDecade;


public class Layer4A 
{
	public static class Layer4A_Mapper extends Mapper<LongWritable, Text, WordWordDecade, DataStructureBase> {
	
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//System.out.println("MAPPING 4A: " + value);
			
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
				System.out.println("EXCEPTION: " + ex.getStackTrace().toString());
				return;
			}
			DSLayer4 ds4 = (DSLayer4) ds;
			DataStructureBase ds5 = DataStructureBase.create(wwdKey.getWord1(),wwdKey.getWord2(), ds4.getPairSum() , ds4.getWord1Sum(), ds4.getWord2Sum());
			//System.out.println("Mapper Output: Key:" + wwdKey.toString() + ", Value:" + ds5.toString());
			context.write(wwdKey, ds5);		
		}
	}
	
	public static class Layer4A_GroupingComparator extends WritableComparator {
	    protected Layer4A_GroupingComparator() {
	        super(WordWordDecade.class, true);
	        //System.out.println("Layer4A_GroupingComparator");
	    }   
	    
	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	WordWordDecade k1 = (WordWordDecade)w1;
	    	WordWordDecade k2 = (WordWordDecade)w2;
	    	//System.out.println("Layer4A_GroupingComparator Integer.compare(k1.getDecade(), k2.getDecade():" + Integer.compare(k1.getDecade(), k2.getDecade()));
			return Integer.compare(k1.getDecade(), k2.getDecade());
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
	
	public static class Layer4A_Reducer extends Reducer<WordWordDecade, DSLayer5, WordWordDecade, DoubleWritable> {
		
		private int k;
		
		public void setup(Context context) {
			//System.out.println("--------------REDUCER 4A SETUP---------------");
			k = new Integer(context.getConfiguration().get("k"));
			//System.out.println("k = " + k);
		}
		
		/**
		 * Puts the pmi and key in their relevant arrays, and keep the arrays ordered (low to high).				
		 * @param pmi The PMI candidate
		 * @param key The WordWordDecade key candidate which scored the pmi
		 * @param orderedPMIs a ascending ordered array, which contains the PMIs
		 * @param orderedKeys and array of keys that co-relate to the PMI array
		 */
		void putValueInOrderedArray(double pmi, WordWordDecade key, double[] orderedPMIs, WordWordDecade[] orderedKeys) {
			
			if (pmi < orderedPMIs[0]) {
				return;
			}
			
			for (int i = 1; i <= orderedPMIs.length; i++) {
				if (i < orderedPMIs.length && orderedPMIs[i] < pmi) {
					orderedPMIs[i-1] = orderedPMIs[i];
					orderedKeys[i-1] = orderedKeys[i];
				} else {
					orderedPMIs[i-1] = pmi;
					orderedKeys[i-1] = key;
					break;
				}
			}
		}
		
		public void reduce(WordWordDecade key, Iterable<DSLayer5> values, Context context)
				throws IOException, InterruptedException {
		
			//System.out.println("Reducing L4A: " + key.toString());
			
			// The total number of words in this decade will be the first value in the iterable
			// That's because we defined the secondary sort to be WordWordDecade's sort, and we defined a grouping comparator		
			long sumWordsInDecade = values.iterator().next().getNum1();			
			
			WordWordDecade[] maxKPmi_keys = new WordWordDecade[k];
			double[] maxKPmi = new double[k];
			int numKeys = 0;
			
			// Calculate for each couple its PMI, and if it's among the k highest PMI scores - put it in the array
			for (DSLayer5 value : values) {				
				WordWordDecade new_wwdKey = new WordWordDecade(value.getWord1(), value.getWord2(), key.getDecade());				
				double pmi = Math.log(value.getNum1()) + Math.log(sumWordsInDecade) - Math.log(value.getNum2()) - Math.log(value.getNum3());
				putValueInOrderedArray(pmi, new_wwdKey, maxKPmi, maxKPmi_keys);
				numKeys++;
				//System.out.println(new_wwdKey.toString() + " " + value.getNum1() + " " + value.getNum2() + " " + value.getNum3() + " " + sumWordsInDecade + " " + pmi);
			}	
			
			// Writes to context the max k couples which scored the highest PMIs
			int min = Math.min(k, numKeys);
			for (int i = k-1; i >= k - min; --i) {
				DoubleWritable pmiCalcW = new DoubleWritable(maxKPmi[i]);
				WordWordDecade wwdKey =  maxKPmi_keys[i];	
				//System.out.println("Writing - Key: " + wwdKey.toString() + ", Value: " + pmiCalcW.toString());
				context.write(wwdKey, pmiCalcW);
			}	
		}
	}

	  public static void main(String[] args) throws Exception {
		  	System.out.println("RUNNING L4a");	
			//System.out.println("args[0]:" + args[0].toString() + "; args[1]:" + args[1].toString() + "; args[2]:" + args[2].toString() + "; args[3]:" + args[3].toString());

		    Configuration conf = new Configuration();
		    conf.set("k", args[3]);
		    Job job = Job.getInstance(conf, "ass2");
		    job.setJarByClass(Layer4A.class);
		    job.setMapperClass(Layer4A_Mapper.class);
		    job.setGroupingComparatorClass(Layer4A.Layer4A_GroupingComparator.class);
		   // job.setCombinerClass(Layer4_Reducer.class);
		    job.setReducerClass(Layer4A_Reducer.class);
		    job.setPartitionerClass(PartitionerClass.class);
		    job.setMapOutputKeyClass(WordWordDecade.class);
		    job.setMapOutputValueClass(DSLayer5.class);
		    job.setOutputKeyClass(WordWordDecade.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    //job.setInputFormatClass(SequenceFileInputFormat.class);
		    FileInputFormat.addInputPath(job, new Path(args[1]));
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}