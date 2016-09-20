package DataStructures;

/**
 * MyDataStructure is a data structure that contains 3 numbers, and 2 strings 
 * It is easier to pass this DS in mappers/reducers output
 * @author Or, Yoed
 *
 */

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;

public abstract class DataStructureBase implements Writable {

	protected final static String EMPTY_STR = "~"; // Should be char with high ASCII value
	
	///////////////////////////////////////////////////
	// Factory methods()
	///////////////////////////////////////////////////
	
	public static DataStructureBase create(long number) {
		return new DSLayer1(number);
	}
	
	public static DataStructureBase create(String word, long number) {
		return new DSLayer2(word, number);
	}
	
	public static DataStructureBase create(long num1, long num2) {
		return new DSLayer3(num1, num2);
	}
	
	public static DataStructureBase create(long num1, long num2 , long num3) {
		return new DSLayer4(num1, num2, num3);
	}
	
	public static DataStructureBase create(String word1, String word2, long num1, long num2, long num3) {
		return new DSLayer5(word1, word2, num1, num2, num3);
	}
	
	public static DataStructureBase create(long num1, long num2, long num3, long num4, double num5) {
		return new DSLayer6(num1, num2, num3, num4, num5 );
	}

	/**
	 * Parse a string to DataStructureBase. The input value is assumed to be valid
	 */
	public static DataStructureBase parse(String value) throws Exception {		
		String[] splt = value.split(" ");			
		switch (splt.length) {
			case 1: // e.g 1000
				return DataStructureBase.create(Long.valueOf(splt[0]));
			case 2: 
				try  // e.g 1000 120040
				{  				    
					return DataStructureBase.create(Long.valueOf(splt[0]), Long.valueOf(splt[1]));
				}  
				catch(NumberFormatException nfe) // e.g home 1000  
				{  
					return DataStructureBase.create(splt[0], Long.valueOf(splt[1]));  
				}  								
			case 4:		
				return DataStructureBase.create(Long.valueOf(splt[0]), Long.valueOf(splt[1]));
			default:
				throw new Exception("Invalid String, parsing failed");
		}
	}
	/**
	 * Creates a copy of this data structure
	 */
	public abstract DataStructureBase copy();
}
