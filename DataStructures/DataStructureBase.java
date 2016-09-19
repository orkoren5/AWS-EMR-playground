package DataStructures;

/**
 * MyDataStructure is a data structure that contains 3 numbers, and 2 strings 
 * It is easier to pass this DS in mappers/reducers output
 * @author Or, Yoed
 *
 */

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
	 * Creates a copy of this data structure
	 */
	public abstract DataStructureBase copy();
}
