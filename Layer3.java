import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import DataStructures.DSLayer1;
import DataStructures.DSLayer2;
import DataStructures.DSLayer3;
import DataStructures.DataStructureBase;
import DataStructures.WordWordDecade;


public class Layer3 {

	public static class Layer3_Mapper extends Mapper<LongWritable, Text, WordWordDecade, DataStructureBase> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] keyValue = value.toString().split("\t");
			String str_wwdKey = keyValue[0];
			String count = keyValue[1];
			WordWordDecade wwdKey = null;
			
			try {
				wwdKey = WordWordDecade.parse(str_wwdKey);
			}
			catch( Exception ex) {
				System.out.println("EXCEPTION: " + ex);
				return;
			}

			System.out.println("Mapped: " + value);
			DataStructureBase ds = DataStructureBase.create(wwdKey.getWord2(), Long.parseLong(count));
			wwdKey.clearWord2();
			context.write(wwdKey, ds);
		}

	}
	
	public static class Layer3_Reducer extends Reducer<WordWordDecade, DataStructureBase, WordWordDecade, DataStructureBase> {
		
		/**
		 * The values which Layer2 reducer gets is design to have only one DS type of layer 1, which is Long value.
		 * That value is the sum of all appearances of word1 in a decade.
		 * @param values the list of values the reducer gets
		 * @return That value of the sum of all appearances of word1 in a decade.
		 */
		long getTheOnlyNumberValue(Iterable<DataStructureBase> values) {
			for (DataStructureBase value : values) {			
				if (value instanceof DSLayer1) {
					return ((DSLayer1)value).getNumber();
				}
			}
			return 0;
		}
		
		public void reduce(WordWordDecade key, Iterable<DataStructureBase> values, Context context)
				throws IOException, InterruptedException {
		
			System.out.println("Reducing: " + key.toString());
			long totalNumberOfWord1 = getTheOnlyNumberValue(values);
			for (DataStructureBase value : values) {
				DSLayer2 l2value = (DSLayer2)value;
				WordWordDecade new_wwdKey = new WordWordDecade(key.getWord1(), l2value.getWord(), key.getDecade());
				DSLayer3 new_value = new DSLayer3(l2value.getNumber(), totalNumberOfWord1);
				context.write(new_wwdKey, new_value);
			}
	}
}

}