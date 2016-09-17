package DataStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DSLayer4 extends DataStructureBase  {
	
	String word1;
	String word2;
	long num1;
	long num2;			
	long num3;
	
	public DSLayer4() {
		
	}
	
	public DSLayer4(String word1, String word2, long num1, long num2, long num3) {
		this.word1 = word1;
		this.word2 = word2;
		this.num1 = num1;
		this.num2 = num2;
		this.num3 = num3;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.word1 = arg0.readUTF();
		this.word2 = arg0.readUTF();
		this.num1 = arg0.readLong();
		this.num2 = arg0.readLong();
		this.num3 = arg0.readLong();
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(this.word1);
		arg0.writeUTF(this.word2);
		arg0.writeLong(this.num1);
		arg0.writeLong(this.num2);
		arg0.writeLong(this.num3);
	}

	@Override
	public DataStructureBase copy() {
		return new DSLayer4(this.word1, this.word2, this.num1, this.num2, this.num3);
	}	
}
