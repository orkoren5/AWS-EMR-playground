
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import DataStructures.WordWordDecade;



public class Driver 
{
	public static void main(String[] args) throws Exception 
	{
		System.out.println("Run Driver");
		
		int serialNum = 56;
		
		Job jobLayer1 = initLayer1Job(args[0], args[1] + serialNum);
		jobLayer1.waitForCompletion(true);
		
		Job jobLayer2 = initLayer2Job(args[1] + serialNum , args[1] + serialNum + "b");
		jobLayer2.waitForCompletion(true);
		
		//Job jobLayer3 = initLayer2Job();
		//jobLayer3.waitForCompletion(true);


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
	    //job.setInputFormatClass(SequenceFileInputFormat.class);
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
	    job.setCombinerClass(Layer2.Layer2_Reducer.class);
	    job.setReducerClass(Layer2.Layer2_Reducer.class);
	    job.setOutputKeyClass(WordWordDecade.class);
	    job.setOutputValueClass(LongWritable.class);
	    //job.setInputFormatClass(SequenceFileInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    
	    System.out.println("job Layer2 created");
	    
	    return job;
	    	
	}
	
}
