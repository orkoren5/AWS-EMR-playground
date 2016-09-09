
import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.collect.Lists;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class Layer1 {

  public static class LayerOneMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	//split ngrams year amount and bla bla
    	String[] splitted = value.toString().split("\t");	
    	//if broken line
    	if (splitted.length < 3) 
    		return;
    	
    	int year = Integer.parseInt(splitted[1]);
    	//ck valid year
    	if (year < 1900) 
    		return;
    	
    	Long amount = Long.parseLong(splitted[2]);
    	
    	String[] ngrams = splitted[0].split(" ");
    	List<String> validWords = Lists.newArrayList();
    	for (int i = 0; i < ngrams.length; i++) {
    		String word = cleanNgrams(ngrams[i]);
    		if (word.length() > 1 && !isStopWord(word)) {
					validWords.add(word);
				}
    		
    	}
    	
    	
    	
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
    
    public String cleanNgrams(String ngram) {
    	ngram = ngram.toLowerCase();
		if (ngram.endsWith("'s"))
			ngram = ngram.substring(0,ngram.length()-2);
		ngram = ngram.replaceAll("'","");
		ngram = ngram.replaceAll("[^a-zA-Z0-9']",""); //same as \\w
		if ((ngram.length() < 2) || !Character.isLetter(ngram.charAt(0)))
			ngram = "";
		return ngram;
    }
    
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCountpro.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //job.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


}
