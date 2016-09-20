
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




import DataStructures.WordWordDecade;


public class Layer1 {

  public static class Layer1Mapper
       extends Mapper<Object, Text, WordWordDecade, LongWritable>{
    private Set<String> stopWords = new HashSet<String>(Arrays.asList("", "a", "able", "about", "above", "abst", "accordance",
			"according", "accordingly", "across", "act", "actually", "added", "adj", "affected", "affecting",
			"affects", "after", "afterwards", "again", "against", "ah", "all", "almost", "alone", "along",
			"already", "also", "although", "always", "am", "among", "amongst", "an", "and", "announce", "another",
			"any", "anybody", "anyhow", "anymore", "anyone", "anything", "anyway", "anyways", "anywhere",
			"apparently", "approximately", "are", "aren", "arent", "arise", "around", "as", "aside", "ask",
			"asking", "at", "auth", "available", "away", "awfully", "b", "back", "be", "became", "because",
			"become", "becomes", "becoming", "been", "before", "beforehand", "begin", "beginning", "beginnings",
			"begins", "behind", "being", "believe", "below", "beside", "besides", "between", "beyond", "biol",
			"both", "brief", "briefly", "but", "by", "c", "ca", "came", "can", "cannot", "can't", "cause", "causes",
			"certain", "certainly", "co", "com", "come", "comes", "contain", "containing", "contains", "could",
			"couldnt", "d", "date", "did", "didn't", "different", "do", "does", "doesn't", "doing", "done", "don't",
			"down", "downwards", "due", "during", "e", "each", "ed", "edu", "effect", "eg", "eight", "eighty",
			"either", "else", "elsewhere", "end", "ending", "enough", "especially", "et", "et-al", "etc", "even",
			"ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "except", "f", "far", "few",
			"ff", "fifth", "first", "five", "fix", "followed", "following", "follows", "for", "former", "formerly",
			"forth", "found", "four", "from", "further", "furthermore", "g", "gave", "get", "gets", "getting",
			"give", "given", "gives", "giving", "go", "goes", "gone", "got", "gotten", "h", "had", "happens",
			"hardly", "has", "hasn't", "have", "haven't", "having", "he", "hed", "hence", "her", "here",
			"hereafter", "hereby", "herein", "heres", "hereupon", "hers", "herself", "hes", "hi", "hid", "him",
			"himself", "his", "hither", "home", "how", "howbeit", "however", "hundred", "i", "id", "ie", "if",
			"i'll", "im", "immediate", "immediately", "importance", "important", "in", "inc", "indeed", "index",
			"information", "instead", "into", "invention", "inward", "is", "isn't", "it", "itd", "it'll", "its",
			"itself", "i've", "j", "just", "k", "keep	keeps", "kept", "kg", "km", "know", "known", "knows", "l",
			"largely", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets",
			"like", "liked", "likely", "line", "little", "'ll", "look", "looking", "looks", "ltd", "m", "made",
			"mainly", "make", "makes", "many", "may", "maybe", "me", "mean", "means", "meantime", "meanwhile",
			"merely", "mg", "might", "million", "miss", "ml", "more", "moreover", "most", "mostly", "mr", "mrs",
			"much", "mug", "must", "my", "myself", "n", "na", "name", "namely", "nay", "nd", "near", "nearly",
			"necessarily", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine",
			"ninety", "no", "nobody", "non", "none", "nonetheless", "noone", "nor", "normally", "nos", "not",
			"noted", "nothing", "now", "nowhere", "o", "obtain", "obtained", "obviously", "of", "off",
			"oh", "ok", "okay", "old", "omitted", "on", "once", "one", "ones", "only", "onto", "or", "ord", "other",
			"others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall",
			"owing", "own", "p", "page", "pages", "part", "particular", "particularly", "past", "per", "perhaps",
			"placed", "please", "plus", "poorly", "possible", "possibly", "potentially", "pp", "predominantly",
			"present", "previously", "primarily", "probably", "promptly", "proud", "provides", "put", "q", "que",
			"quickly", "quite", "qv", "r", "ran", "rather", "rd", "re", "readily", "really", "recent", "recently",
			"ref", "refs", "regarding", "regardless", "regards", "related", "relatively", "research",
			"respectively", "resulted", "resulting", "results", "right", "run", "s", "said", "same", "saw", "say",
			"saying", "says", "sec", "section", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen",
			"self", "selves", "sent", "seven", "several", "shall", "she", "shed", "she'll", "shes", "should",
			"shouldn't", "show", "showed", "shown", "showns", "shows", "significant", "significantly", "similar",
			"similarly", "since", "six", "slightly", "so", "some", "somebody", "somehow", "someone", "somethan",
			"something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specifically",
			"specified", "specify", "specifying", "still", "stop", "strongly", "sub", "substantially",
			"successfully", "such", "sufficiently", "suggest", "sup", "sure	t", "take", "taken", "taking", "tell",
			"tends", "th", "than", "thank", "thanks", "thanx", "that", "that'll", "thats", "that've", "the",
			"their", "theirs", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "thered",
			"therefore", "therein", "there'll", "thereof", "therere", "theres", "thereto", "thereupon", "there've",
			"these", "they", "theyd", "they'll", "theyre", "they've", "think", "this", "those", "thou", "though",
			"thoughh", "thousand", "throug", "through", "throughout", "thru", "thus", "til", "tip", "to",
			"together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "ts",
			"twice", "two", "u", "un", "under", "unfortunately", "unless", "unlike", "unlikely", "until", "unto",
			"up", "upon", "ups", "us", "use", "used", "useful", "usefully", "usefulness", "uses", "using",
			"usually", "v", "value", "various", "'ve", "very", "via", "viz", "vol", "vols", "vs", "w", "want",
			"wants", "was", "wasnt", "way", "we", "wed", "welcome", "we'll", "went", "were", "werent", "we've",
			"what", "whatever", "what'll", "whats", "when", "whence", "whenever", "where", "whereafter", "whereas",
			"whereby", "wherein", "wheres", "whereupon", "wherever", "whether", "which", "while", "whim", "whither",
			"who", "whod", "whoever", "whole", "who'll", "whom", "whomever", "whos", "whose", "why", "widely",
			"willing", "wish", "with", "within", "without", "wont", "words", "world", "would", "wouldnt", "www",
			"x", "y", "yes", "yet", "you", "youd", "you'll", "your", "youre", "yours", "yourself", "yourselves",
			"you've", "z", "zero","a","about","above","after","again","against","all","am","an",
			"and","any","are","aren't","as","at","be","because","been","before","being",
			"below","between","both","but","by","can't","cannot","could","couldn't",
			"did","didn't","do","does","doesn't","doing","don't","down","during","each",
			"few","for","from","further","had","hadn't","has","hasn't","have","haven't",
			"having","he","he'd","he'll","he's","her","here","here's","hers","herself",
			"him","himself","his","how","how's","i","i'd","i'll","i'm","i've","if","in",
			"into","is","isn't","it","it's","its","itself","let's","me","more","most",
			"mustn't","my","myself","no","nor","not","of","off","on","once","only","or",
			"other","ought","our","ours","ourselves","out","over","own","same","shan't",
			"she","she'd","she'll","she's","should","shouldn't","so","some","such","than",
			"that","that's","the","their","theirs","them","themselves","then","there","there's",
			"these","they","they'd","they'll","they're","they've","this","those","through","to",
			"too","under","until","up","very","was","wasn't","we","we'd","we'll","we're","we've",
			"were","weren't","what","what's","when","when's","where","where's","which","while","who",
			"who's","whom","why","why's","with","won't","would","wouldn't","you","you'd","you'll",
			"you're","you've","your","yours","yourself","yourselves",
			"\"","'","^","?",";",":","1",".","-","*","#","$","&","%","!",")","("));
    

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    	System.out.println("#### MAPPING: " + value.toString());
    	
    	//split ngrams year amount and bla bla
    	String[] splitted = value.toString().split("\t");	
    	//if broken line
    	if (splitted.length < 3)
    	{ 
    		System.out.println("splitted.length:" + splitted.length + " < 3: -> return");
    		return;
    	}
    	
    	int decade = (Integer.parseInt(splitted[1])/10)*10;
    	System.out.println("decade: " + decade);
    	//ck valid year
    	if (decade < 1900)
    	{
    		System.out.println("decade < 1900: -> return");
    		return;
    	}
    	//save amount of ngram
    	long ngram_amoung = Long.parseLong(splitted[2]);    	

    	//clean stop words and signs - save valid to validWords
    	String[] ngrams = splitted[0].split(" ");

    	ArrayList<String> validWords = new ArrayList<String>();
    	for (int i = 0; i < ngrams.length; i++) {
    		String word = cleanNgrams(ngrams[i]);
    		if (word.length() > 1 && !stopWords.contains(word)) {
					validWords.add(word);
				}
    	}
    	
    	//split the validWords to <key,val> for the Reducer
    	int size = validWords.size();
    	if(size > 0)
    	{
    		LongWritable amount = new LongWritable(ngram_amoung*size);
			int midWordIndex = (int) Math.ceil(size / 2.0);
			String middleWord = validWords.remove(midWordIndex);
	
			//<{*,*,decade},amount>
			WordWordDecade emptyPair = new WordWordDecade(decade);
			System.out.println("Mapper Output emptyPair- Key:" + emptyPair.toString() + ", Value:" + amount.toString());
			context.write(emptyPair , amount);
			
			WordWordDecade wordMiddle = new WordWordDecade(middleWord,decade);
			System.out.println("Mapper Output wordMiddle: Key:" + wordMiddle.toString() + ", Value " + amount.toString());
			context.write(wordMiddle , amount);
		
			
			for(String word : validWords)
			{
				//<{middleWord,wi,decade},amount>
				// value of c(w,wi) or c(wi,w)
				WordWordDecade wordPair = new WordWordDecade(middleWord,word,decade);
				System.out.println("Mapper Output words: Key:" + wordPair.toString() + ", Value " + amount.toString());
				context.write(wordPair , amount);
				
				WordWordDecade wordSingle = new WordWordDecade(word,decade);
				System.out.println("Mapper Output wordSingle: Key:" + wordSingle.toString() + ", Value " + amount.toString());
				context.write(wordSingle , amount);
		
			}
    	}
    }
    
    public String cleanNgrams(String ngram) {
    	ngram = ngram.toLowerCase();
		if (ngram.endsWith("'s"))
			ngram = ngram.substring(0,ngram.length()-2);
		ngram = ngram.replaceAll("'","");
		ngram = ngram.replaceAll("[^a-zA-Z0-9]",""); //same as \\w
		if ((ngram.length() < 2) || !Character.isLetter(ngram.charAt(0)))
			ngram = "";
		return ngram;
    }
    
  }

  
  public static class PartitionerClass extends Partitioner<WordWordDecade, LongWritable>
	{
		@Override
		public int getPartition(WordWordDecade key, LongWritable value, int numPartitions)
		{
			int decade = key.getDecade() + 2; 
			int decadeToPrint = decade % 12;
			return decadeToPrint; //12 - num of decade from 1900 to 2020
		}
	}
  
  
  public static class LayerOneReducer
       extends Reducer<WordWordDecade,LongWritable,WordWordDecade,LongWritable> 
  {

    public void reduce(WordWordDecade key, Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException 
    {
       System.out.println("$$ Reducing: " + key.toString());	
	   long sum = 0;
	   //sum all val
	   for (LongWritable val : values) 
	   {
	     sum += val.get();
	   }
	   //store
	   LongWritable sumToPrint = new LongWritable(sum);
	   System.out.println("Writing - Key: " + key.toString() + ", Value: " + sumToPrint.toString());
	   context.write(key, sumToPrint); 
    }
  }

  public static void main(String[] args) throws Exception {	 
	System.out.println("RUNNING L1");	
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "ass2");
    job.setJarByClass(Layer1.class);
    job.setMapperClass(Layer1Mapper.class);
    job.setCombinerClass(LayerOneReducer.class);
    job.setReducerClass(LayerOneReducer.class);
    job.setOutputKeyClass(WordWordDecade.class);
    job.setOutputValueClass(LongWritable.class);
    //job.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


}
