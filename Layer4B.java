import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashSet;

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


public class Layer4B 
{
	public static class Layer4B_Mapper extends Mapper<LongWritable, Text, WordWordDecade, DataStructureBase> {
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//System.out.println("MAPPING L4B: " + value);
			
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
			
			if (wwdKey.getDecade() == 2000) {
				DSLayer4 ds4 = (DSLayer4) ds;
				DataStructureBase ds5 = DataStructureBase.create(wwdKey.getWord1(),wwdKey.getWord2(), ds4.getPairSum() , ds4.getWord1Sum(), ds4.getWord2Sum());
				//System.out.println("Mapper Output: Key:" + wwdKey.toString() + ", Value:" + ds5.toString());
				context.write(wwdKey, ds5);		
			}
		}
	}
	
	public static class Layer4B_GroupingComparator extends WritableComparator {
	    protected Layer4B_GroupingComparator() {
	        super(WordWordDecade.class, true);
	        //System.out.println("Layer4B_GroupingComparator");
	    }   
	    
	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	WordWordDecade k1 = (WordWordDecade)w1;
	    	WordWordDecade k2 = (WordWordDecade)w2;
	    	//System.out.println("Layer4B_GroupingComparator Integer.compare(k1.getDecade(), k2.getDecade():" + Integer.compare(k1.getDecade(), k2.getDecade()));	    	
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
					//System.out.println("PartitionerClass decadeToPrint:" + decadeToPrint + "key:" + key.toString() + "value:" + value.toString() );
					return decadeToPrint % numPartitions; //12 - num of decade from 1900 to 2020
					
				}
			}
	
	public static class Layer4B_Reducer extends Reducer<WordWordDecade, DSLayer5, Text, DoubleWritable> {
		
		HashSet<WordWordDecade> related;
		HashSet<WordWordDecade> notRelated;
		double threshold;
		
		/**
		 * Setup before mapping - Gets the related/non-related pairs from the web and puts them in a HashSet
		 */
		public void setup(Context context) {
			//System.out.println("--------------MAPPER L4B SETUP: Get related/non-related pairs from web----------------");
			related = getPairsFromTestSetWeb("https://www.cs.bgu.ac.il/~dsp162/wiki.files/wordsim-pos.txt");
			notRelated = getPairsFromTestSetWeb("https://www.cs.bgu.ac.il/~dsp162/wiki.files/wordsim-neg.txt");
			threshold = new Double(context.getConfiguration().get("threshold", "1"));
			System.out.println("related: " +related.toString());
			System.out.println("notRelated: " + notRelated.toString());
		}
		
		
		private HashSet<WordWordDecade> getPairsFromTestSetWeb(String urlStr) {
			HashSet<WordWordDecade> pairs = new HashSet<WordWordDecade>();
	        URL url;
			try {
				url = new URL(urlStr);
				BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
				String inputLine;
		        while ((inputLine = in.readLine()) != null) {
		        	String[] splt = inputLine.split("\t");
		        	WordWordDecade wwd = new WordWordDecade(splt[0], splt[1], 2000);
		        	pairs.add(wwd);
		        }
		        in.close();		        
			} catch (Exception e) {	
				System.out.println("EXCEPTION: " + e);				
				System.out.println("EXCEPTION: " + e.getStackTrace().toString());
				e.printStackTrace();				
			} 	 
			return pairs;
		}
		
		private double calcFMeasure(int tp, int tn, int fp, int fn) {
			System.out.println("calcFMeasure; int tp, int tn, int fp, int fn :" + tp + "," + tn + "," + fp + "," + fn );
			if(tp == 0)
				return 0;
			double precision = tp / (tp + fp);
			double recall = tp / (tp + fn);
			System.out.println("precision: " + precision + " recall: " + recall);
			return 2 * (precision * recall) / (precision + recall);
		}
		
		public void reduce(WordWordDecade key, Iterable<DSLayer5> values, Context context)
				throws IOException, InterruptedException {
		
			//System.out.println("Reducing L4B: " + key.toString());
			
			// The total number of words in this decade will be the first value in the iterable
			// That's because we defined the secondary sort to be WordWordDecade's sort, and we defined a grouping comparator		
			long sumWordsInDecade = values.iterator().next().getNum1();						
			int tp = 0, tn = 0, fp = 0, fn = 0;
			
			// Calculate for each couple its PMI, determine its classification (TP, TN..) and sum it
			for (DSLayer5 value : values) {				
				WordWordDecade new_wwdKey = new WordWordDecade(value.getWord1(), value.getWord2(), key.getDecade());				
				double pmi = Math.log(value.getNum1()) + Math.log(sumWordsInDecade) - Math.log(value.getNum2()) - Math.log(value.getNum3());
				//System.out.println("value: "+ value.toString() + " PMI " + pmi);
				if (pmi >= threshold && related.contains(new_wwdKey)) {
					System.out.println(new_wwdKey.toString() + ": PMI " + pmi + " TP");
					++tp;
				} else if (pmi < threshold && notRelated.contains(new_wwdKey)) {
					System.out.println(new_wwdKey.toString() + ": PMI " + pmi + " TN");
					++tn;
				} else if (pmi >= threshold && notRelated.contains(new_wwdKey)) {
					System.out.println(new_wwdKey.toString() + ": PMI " + pmi + " FN");
					++fp;
				} else if (pmi < threshold && related.contains(new_wwdKey)) {
					System.out.println(new_wwdKey.toString() + ": PMI " + pmi + " FP");
					++fn;
				}					
			}	
			
			double FMeasure = calcFMeasure(tp, tn, fp, fn);
			System.out.println("FMeasure: " + FMeasure );
			context.write(new Text("Threshold"), new DoubleWritable((double)threshold));
			context.write(new Text("TP"), new DoubleWritable((double)tp));
			context.write(new Text("TN"), new DoubleWritable((double)tn));
			context.write(new Text("FP"), new DoubleWritable((double)fp));
			context.write(new Text("FN"), new DoubleWritable((double)fn));		
			context.write(new Text("FMeasure"), new DoubleWritable(FMeasure));
		}
	}

	  public static void main(String[] args) throws Exception {
		  	System.out.println("RUNNING L4b");	
			System.out.println("args[0]:" + args[0].toString() + "; args[1]:" + args[1].toString() + "; args[2]:" + args[2].toString() + "; args[3]:" + args[3].toString());

		  	Configuration conf = new Configuration();
		    conf.set("threshold", args[3]);
		    Job job = Job.getInstance(conf, "ass2");
		    job.setJarByClass(Layer4B.class);
		    job.setMapperClass(Layer4B.Layer4B_Mapper.class);
		    //job.setCombinerClass(Layer3.Layer3_Reducer.class);
		    job.setGroupingComparatorClass(Layer4B.Layer4B_GroupingComparator.class);
		    job.setReducerClass(Layer4B.Layer4B_Reducer.class);
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