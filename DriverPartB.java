
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import DataStructures.DSLayer2;
import DataStructures.DSLayer3;
import DataStructures.DSLayer4;
import DataStructures.DSLayer5;
import DataStructures.WordWordDecade;

public class DriverPartB 
{
	public static void main(String[] args) throws Exception 
	{
		System.out.println("Run Driver");
				
		String outputFolder = args[1];
		
		Job jobLayer1 = initLayer1Job(args[0], outputFolder);
		jobLayer1.waitForCompletion(true);
		
		Job jobLayer2 = initLayer2Job(outputFolder, outputFolder + "b");
		jobLayer2.waitForCompletion(true);
		
		Job jobLayer3 = initLayer3Job(outputFolder + "b", outputFolder + "c");
		jobLayer3.waitForCompletion(true);
		
		Job jobLayer4 = initLayer4Job(outputFolder + "c", outputFolder + "d");
		jobLayer4.waitForCompletion(true);
		
		Job jobLayer5 = initLayer5Job(outputFolder + "d", outputFolder + "e", args[2]);
		jobLayer5.waitForCompletion(true);
		

		System.exit(0);

	}
	
	public static Job initLayer1Job(String input, String output) throws IOException 
	{
		System.out.println("init Layer1 job");
		
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "ass2");
	    job.setJarByClass(Layer1.class);
	    job.setMapperClass(Layer1.Layer1Mapper.class);
	    job.setCombinerClass(Layer1.LayerOneReducer.class);
	    job.setReducerClass(Layer1.LayerOneReducer.class);
	    job.setOutputKeyClass(WordWordDecade.class);
	    job.setOutputValueClass(LongWritable.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    
	    System.out.println("job Layer1 created");
	    return job;
	    	
	}
	
	public static Job initLayer2Job(String input, String output) throws IOException 
	{
		System.out.println("init Layer2 job");
		
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "ass2");
	    job.setJarByClass(Layer2.class);
	    job.setMapperClass(Layer2.Layer2_Mapper.class);
	    //job.setCombinerClass(Layer2.Layer2_Reducer.class);
	    job.setGroupingComparatorClass(Layer2.Layer2_GroupingComparator.class);
	    job.setReducerClass(Layer2.Layer2_Reducer.class);
	    job.setMapOutputKeyClass(WordWordDecade.class);
	    job.setMapOutputValueClass(DSLayer2.class);
	    job.setOutputKeyClass(WordWordDecade.class);
	    job.setOutputValueClass(DSLayer3.class);
	    //job.setInputFormatClass(SequenceFileInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    
	    System.out.println("job Layer2 created");
	    
	    return job;
	    	
	}
	
	public static Job initLayer3Job(String input, String output) throws IOException 
	{
		System.out.println("init Layer3 job");
		
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "ass2");
	    job.setJarByClass(Layer3.class);
	    job.setMapperClass(Layer3.Layer3_Mapper.class);
	    //job.setCombinerClass(Layer3.Layer3_Reducer.class);
	    job.setReducerClass(Layer3.Layer3_Reducer.class);
	    job.setMapOutputKeyClass(WordWordDecade.class);
	    job.setMapOutputValueClass(DSLayer3.class);
	    job.setOutputKeyClass(WordWordDecade.class);
	    job.setOutputValueClass(DSLayer4.class);
	    //job.setInputFormatClass(SequenceFileInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    
	    System.out.println("job Layer3 created");
	    
	    return job;
	    	
	}
	
	public static Job initLayer4Job(String input, String output) throws IOException 
	{
		System.out.println("init Layer4B job");
		
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "ass2");
	    job.setJarByClass(Layer4B.class);
	    job.setMapperClass(Layer4B.Layer4_Mapper.class);
	    //job.setCombinerClass(Layer3.Layer3_Reducer.class);
	    job.setGroupingComparatorClass(Layer4B.Layer4_GroupingComparator.class);
	    job.setReducerClass(Layer4B.Layer4_Reducer.class);
	    job.setMapOutputKeyClass(WordWordDecade.class);
	    job.setMapOutputValueClass(DSLayer5.class);
	    job.setOutputKeyClass(WordWordDecade.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    //job.setInputFormatClass(SequenceFileInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    
	    System.out.println("job Layer4B created");
	    
	    return job;
	    	
	}
	
	public static Job initLayer5Job(String input, String output, String tH) throws IOException 
	{	
		System.out.println("init Layer5 job");
		
	    Configuration conf = new Configuration();
	    conf.set("tH", tH);
	    Job job = Job.getInstance(conf, "ass2");
	    job.setJarByClass(Layer5.class);
	    job.setMapperClass(Layer5.Layer5_Mapper.class);
	    //job.setGroupingComparatorClass(Layer5.Layer5_GroupingComparator.class);
	    job.setReducerClass(Layer5.Layer5Reducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    
	    System.out.println("job Layer5 created");
	    
	    return job;
	    	
	}
	

}