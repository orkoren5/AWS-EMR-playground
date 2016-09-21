package DataStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DSLayer3 extends DataStructureBase  {
	
	long pairSum;
	long wordSum;			
	
	public DSLayer3() {
		
	}
	
	public DSLayer3(long pairSum, long wordSum) {
		this.pairSum = pairSum;
		this.wordSum = wordSum;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.pairSum = arg0.readLong();
		this.wordSum = arg0.readLong();
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeLong(this.pairSum);
		arg0.writeLong(this.wordSum);
	}	
	
	public long getPairSum() {
		return this.pairSum;
	}

	public long getWordSum() {
		return this.wordSum;
	}
	
	public String toString() {
		return String.valueOf(pairSum) + " " + wordSum;
	}

	@Override
	public DataStructureBase copy() {
		return new DSLayer3(this.pairSum, this.wordSum);
	}	
}
