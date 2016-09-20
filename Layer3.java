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

		/**
		 * This mapper does nothing actually.. only passes the key-values as is to the reducer
		 */
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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
				return;
			}

			System.out.println("Mapped: " + value);			
			context.write(wwdKey, ds);
		}

	}
	
	public static class Layer3_Reducer extends Reducer<WordWordDecade, DSLayer3, WordWordDecade, DataStructureBase> {
			
		public void reduce(WordWordDecade key, Iterable<DSLayer3> values, Context context)
				throws IOException, InterruptedException {
		
			System.out.println("Reducing: " + key.toString());
			for (DSLayer3 value : values) {				
				
			}
			context.write(key, new_value);
	}
}

}