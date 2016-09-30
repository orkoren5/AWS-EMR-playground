
import java.io.IOException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import DataStructures.WordWordDecade;



	public class Layer5 
	{

		public static class Layer5_Mapper extends Mapper<LongWritable, Text,Text,IntWritable>
		{
			public static Set<String> related = new HashSet<String>(Arrays.asList("dramatic,single","tiger,jaguar", "tiger,feline",
					"closet,clothes", "planet,sun", "hotel,reservation", "planet,constellation", "credit,card", "stock,market",
					"psychology,psychiatry", "planet,moon", "planet,galaxy", "bank,money", "physics,proton", "vodka,brandy",
					"war,troops", "Harvard,Yale", "news,report", "psychology,Freud", "money,wealth", "man,woman",
					"FBI,investigation", "network,hardware", "nature,environment", "seafood,food", "weather,forecast",
					"championship,tournament", "law,lawyer", "money,dollar", "calculation,computation", "planet,star",
					"Jerusalem,Israel", "vodka,gin", "money,bank", "computer,software", "murder,manslaughter", "king,queen",
					"OPEC,oil", "Maradona,football", "mile,kilometer", "seafood,lobster", "furnace,stove",
					"environment,ecology", "boy,lad", "asylum,madhouse", "street,avenue", "car,automobile", "gem,jewel",
					"type,kind", "magician,wizard", "football,soccer", "money,currency", "money,cash", "coast,shore",
					"money,cash", "dollar,buck", "journey,voyage", "midday,noon", "tiger,tiger"));
			public static Set<String> unrelated = new HashSet<String>(Arrays.asList("editions,various","king,cabbage", "professor,cucumber",
					"chord,smile", "noon,string", "rooster,voyage", "sugar,approach", "stock,jaguar", "stock,life",
					"monk,slave", "lad,wizard", "delay,racism", "stock,CD", "drink,ear", "stock,phone", "holy,sex",
					"production,hike", "precedent,group", "stock,egg", "energy,secretary", "month,hotel", "forest,graveyard",
					"cup,substance", "possibility,girl", "cemetery,woodland", "glass,magician", "cup,entity", "Wednesday,news",
					"direction,combination", "reason,hypertension", "sign,recess", "problem,airport", "cup,article",
					"Arafat,Jackson", "precedent,collection", "volunteer,motto", "listing,proximity", "opera,industry",
					"drink,mother", "crane,implement", "line,insurance", "announcement,effort", "precedent,cognition",
					"media,gain", "cup,artifact", "Mars,water", "peace,insurance", "viewer,serial", "president,medal",
					"prejudice,recognition", "drink,car", "shore,woodland", "coast,forest", "century,nation",
					"practice,institution", "governor,interview", "money,operation", "delay,news", "morality,importance",
					"announcement,production", "five,month", "school,center", "experience,music", "seven,series", "report,gain",
					"music,project", "cup,object", "atmosphere,landscape", "minority,peace", "peace,atmosphere",
					"morality,marriage", "stock,live", "population,development", "architecture,century",
					"precedent,information", "situation,isolation", "media,trading", "profit,warning", "chance,credibility",
					"theater,history", "day,summer", "development,issue"));
			
			
			private static Double threshold;
			
			
			protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
			{				
				System.out.println("MAPPING L5: " + value);
				
				threshold = Double.parseDouble((context.getConfiguration().get("tH")));
				
				String[] keyValue = value.toString().split("\t");
				String str_wwdKey = keyValue[0];
				Double pmi = Double.valueOf(keyValue[1]);
				WordWordDecade wwdKey = null;
				
				try {
					wwdKey = WordWordDecade.parse(str_wwdKey);
				}
				catch( Exception ex) {
					System.out.println("EXCEPTION: " + ex);				
					System.out.println("EXCEPTION: " + ex.getStackTrace().toString());
					return;
				}
				
				int decade = wwdKey.getDecade();
			
				if (decade != 2000)
				{
					System.out.println("decade != 2000: " + decade);
					return;
				}
				
				String Pairwords = wwdKey.getWord1() + "," + wwdKey.getWord2();
				System.out.println("Pairwords : " + Pairwords);
				
				if (pmi > threshold) 
				{
					if (related.contains(Pairwords)) {
						context.write(new Text("truePositive") ,new IntWritable(1) );
						System.out.println("truePositive");
					} else if (unrelated.contains(Pairwords)) {
						context.write(new Text("falsePositive") ,new IntWritable(1) );
						System.out.println("falsePositive");
					}
				} else 
				{
					if (related.contains(Pairwords)) {
						context.write(new Text("trueNegative") ,new IntWritable(1) );
						System.out.println("trueNegative");
					}
					else if (unrelated.contains(Pairwords)) {
						context.write(new Text("falseNegative") ,new IntWritable(1) );
						System.out.println("falseNegative");
					}
				}
			}
		}
		
		

		public static class PartitionerClass extends Partitioner<Text, IntWritable>
		{
			@Override
			public int getPartition(Text key, IntWritable value, int numPartitions)
			{
				System.out.println(" Partitioner <key,val>: <" + key + "," + value + ">");
				int ret = 0;
				switch (key.toString()) {
				  case "truePositive":
				        ret = 1 ;
				        break;
				  case "falsePositive": 
					  	ret = 2;
				        break;
				  case "trueNegative":
					  	ret = 3;
				      break;
				  case "falseNegative":        
					  	ret = 4;
				        break;
				  default:
				         // executed if none of the above case is satisfied
				        break;   
				}
				System.out.println(" To Partition : " + ret);
				return ret;
			}
		}
	  
	  
	  public static class Layer5Reducer
	       extends Reducer<Text,IntWritable,Text,LongWritable> 
	  {

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	    {
	       System.out.println("$$ Reducing: " + key.toString());	
		   long sum = 0;
		   for (IntWritable val : values) 
		   {
		     sum += val.get();
		   }
		   LongWritable sumToPrint = new LongWritable(sum);
		   System.out.println("Writing - Key: " + key.toString() + ", Value: " + sumToPrint.toString());
		   context.write(key, sumToPrint); 			
	    }
	  }				
	}
		

