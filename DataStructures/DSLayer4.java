package DataStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DSLayer4 extends DataStructureBase  {
	
	long pairSum;
	long word1Sum;
	long word2Sum;
	
	public DSLayer4() {
		
	}
	
	public DSLayer4(long pairSum, long word1Sum, long word2Sum) {
		this.pairSum = pairSum;
		this.word1Sum = word1Sum;
		this.word2Sum = word2Sum;
	}
	
	public long getPairSum() {
		return this.pairSum;
	}
	
	public long getWord1Sum() {
		return this.word1Sum;
	}
	
	public long getWord2Sum() {
		return this.word2Sum;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.pairSum = arg0.readLong();
		this.word1Sum = arg0.readLong();
		this.word2Sum = arg0.readLong();
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeLong(this.pairSum);
		arg0.writeLong(this.word1Sum);
		arg0.writeLong(this.word2Sum);
	}
	
	public String toString() {
		return  String.valueOf(pairSum) + " " + word1Sum + " " + word2Sum;
	}

	@Override
	public DataStructureBase copy() {
		return new DSLayer4(this.pairSum, this.word1Sum, this.word2Sum);
	}	
}
