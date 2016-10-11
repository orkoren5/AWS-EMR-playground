package DataStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WordWordDecade implements WritableComparable<WordWordDecade> {
	private String word1;
	private String word2;
	private Integer decade;
	private final static String EMPTY_STR = "*"; // Should be char with low ASCII value
	
	/**
	 * Creates a new Word-Word-Decade key. 
	 * The former word (Dictionary order) is inserted to word1 field, and the latter to word2 field
	 * An empty word will be the latter.	 	
	 * The year is parsed as decade (1979 -> 1970)
	 * @param wordA a word
	 * @param wordB a word
	 * @param year a year
	 */
	public WordWordDecade(String wordA, String wordB, Integer year) {
		int cmp = wordA.equals(EMPTY_STR) || wordB.equals(EMPTY_STR)
				? -1*wordA.compareTo(wordB)
				: wordA.compareTo(wordB);

		this.word1 = cmp < 0 ? wordA : wordB;
		this.word2 = cmp < 0 ? wordB : wordA;
		this.decade = (year / 10) * 10; // 1971 -> 1970; 1999 -> 1990
	}

	/**
	 * Creates a new Word-Word-Decade key
	 * The word is inserted to word1
	 * @param word a word
	 * @param decade a decade
	 */
	public WordWordDecade(String word, Integer decade) {
		this(word, EMPTY_STR, decade);		
	}

	public WordWordDecade(Integer decade) {
		this(EMPTY_STR, EMPTY_STR, decade);		
	}
	
	public WordWordDecade() {
		this(EMPTY_STR, EMPTY_STR, 0);		
	}
	/**
	 * Parse a string to Word-Word-Decade key. The input value is assumed to be valid
	 */
	public static WordWordDecade parse(String value) throws Exception {		
		String[] splt = value.split(" ");			
		switch (splt.length) {
			case 1: // 1980
				return new WordWordDecade(Integer.valueOf(splt[0]));
			case 2: // home 1980
				return new WordWordDecade(splt[0], Integer.valueOf(splt[1]));
			case 3: // yellow home 1980				
				return new WordWordDecade(splt[0], splt[1], Integer.valueOf(splt[2]));
			default:
				throw new Exception("Invalid String, parsing failed");
		}
	}
	
	public static WordWordDecade copy(WordWordDecade other) {
		return new WordWordDecade(other.word1, other.word2, other.decade);		
	}

	/**
	 * A couple is a key with both words not empty
	 * @return Weather this key is a couple 
	 */
	public boolean isCouple() {
		return !(word1.equals(EMPTY_STR) || word2.equals(EMPTY_STR));
	}
	
	/**
	 * gets weather this key has two empty words
	 * @return Weather this has two empty words
	 */
	public boolean isDecadeOnly() {
		return (word1.equals(EMPTY_STR) && word2.equals(EMPTY_STR));
	}
	
	/**
	 * gets the former word in the key (Dicionary order)
	 */
	public String getWord1(){
		return word1;
	}
	
	/**
	 * gets the latter word in the key (Dicionary order)
	 */
	public String getWord2(){
		return word2;
	}
	
	/**
	 * Switches between word1 and word 2. Note: The dictionary order after swap will not necessary be correct!
	 */
	public void swap() {
		String tmp = this.word1;
		this.word1 = this.word2;
		this.word2 = tmp;
	}
	
	/**
	 * gets the decade
	 */
	public int getDecade(){
		return decade.intValue();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + decade.intValue();
		result = prime * result + word1.hashCode();
		result = prime * result + word2.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if ((obj == null) || (obj.getClass() != this.getClass()))
			return false;
		else 
			return compareTo((WordWordDecade)obj) == 0;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(word1);
		out.writeUTF(word2);
		out.writeInt(decade);
	}

	public void readFields(DataInput in) throws IOException {
		word1 = in.readUTF();
		word2 = in.readUTF();		
		decade = in.readInt();
	}

	public String toString() {
		return this.word1 + " " + this.word2 + " " + this.decade;
	}

	public int compareTo(WordWordDecade o) {
		int res1 = this.decade.compareTo(o.decade);
		int res2 = this.word1.compareTo(o.word1);
		int res3 = this.word2.compareTo(o.word2);
		
		if(res1 != 0) {
			return res1;
		} else if (res2 != 0) {
			return res2;
		} else {
			return res3;
		}		
	}

}