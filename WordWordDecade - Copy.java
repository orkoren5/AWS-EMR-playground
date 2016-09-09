
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WordWordDecade implements WritableComparable<WordWordDecade> {
	private String word1;
	private String word2;
	private Integer decade;

	public WordWordDecade(String word1, String word2, Integer year) {
		this.word1 = word1;
		this.word2 = word2;
		this.decade = (year / 10) * 10; // 1971 -> 1970; 1999 -> 1990
	}

	public WordWordDecade(String word, Integer decade) {
		this(word, "", decade);		
	}

	public WordWordDecade(Integer decade) {
		this("", "", decade);		
	}
	
	public static WordWordDecade parse(String value) throws Exception {
		String[] splt = value.split(" ");	
		switch (splt.length) {
			case 1: // 1980
				return new WordWordDecade(Integer.getInteger(splt[0]));
			case 2: // home 1980
				return new WordWordDecade(splt[0], Integer.getInteger(splt[1]));
			case 3: // yellow home 1980
				return new WordWordDecade(splt[0], splt[1], Integer.getInteger(splt[2]));
			default:
				throw new Exception("Invalid String, parsing failed");
		}
	}
	
	public String getWord1(){
		return word1;
	}

	public String getWord2(){
		return word2;
	}

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