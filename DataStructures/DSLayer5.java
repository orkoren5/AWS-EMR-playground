package DataStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DSLayer5 extends DataStructureBase  {
	
	String word1;
	String word2;
	long num1;
	long num2;			
	long num3;
	
	public DSLayer5() {
		
	}
	
	public DSLayer5(String word1, String word2, long num1, long num2, long num3) {
		this.word1 = word1;
		this.word2 = word2;
		this.num1 = num1;
		this.num2 = num2;
		this.num3 = num3;
	}
	
	public String getWord1() {
		return this.word1.equals(EMPTY_STR) ? "" : this.word1;
	}
	
	public String getWord2() {
		return this.word1.equals(EMPTY_STR) ? "" : this.word2;
	}
	
	public long getNum1() {
		return this.num1;
	}
	
	public long getNum2() {
		return this.num2;
	}
	
	public long getNum3() {
		return this.num3;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.word1 = arg0.readUTF();
		this.word2 = arg0.readUTF();
		this.num1 = arg0.readLong();
		this.num2 = arg0.readLong();
		this.num3 = arg0.readLong();
		
	}
	
	public boolean isWord1Empty() {
		return this.word1.equals(EMPTY_STR);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(this.word1);
		arg0.writeUTF(this.word2);
		arg0.writeLong(this.num1);
		arg0.writeLong(this.num2);
		arg0.writeLong(this.num3);
	}

	public String toString() {
		return  word1 + " " + word2 + " " + String.valueOf(num1) + " " + String.valueOf(num2) + " " + String.valueOf(num3);
	}
	
	@Override
	public DataStructureBase copy() {
		return new DSLayer5(this.word1, this.word2, this.num1, this.num2, this.num3);
	}	
}
