import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Layer2 {

	public static class Layer2_Mapper extends Mapper<LongWritable, Text, WordWordDecade, SeconderySortWritable> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String keyValue = value.toString().split("\t");
			String str_wwdKey = keyValue[0];
			String count = keyValue[1];
			try {
				WordWordDecade wwdKey = WordWordDecade.parse(str_wwdKey);
			}
			catch( Exception ex) {
				System.out.println("EXCEPTION: " + ex);
				return;
			}

			System.out.println("Mapped: " + value);
			WordWordDecade newKey = null;
			SeconderySortWritable valueToWrite = null;
			if (!words.isCouple) {
				keyToWrite = new WordsInDecadeWritable(words.word1 + '$', words.decade);
				valueToWrite = new SeconderySortWritable(count);
			} else {
				keyToWrite = new WordsInDecadeWritable(words.word1, words.decade);
				valueToWrite = new SeconderySortWritable(words.word2, count);
			}
			context.write(keyToWrite, valueToWrite);
		}

	}
}