package DataStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DSLayer6 extends DataStructureBase  {
	
	long pairCount;
	long word1Count;			
	long word2Count;
	long totalNumberInDecade;			
	double pmi;
	
	public DSLayer6() {
		
	}
	
	public DSLayer6(long pairCount, long word1Count, long word2Count, long totalNumberInDecade, double pmi) {

		this.pairCount = pairCount;
		this.word1Count = word1Count;
		this.word2Count = word2Count;
		this.totalNumberInDecade = totalNumberInDecade;
		this.pmi = pmi;
	}
	
	public long getPairCount() {
		return this.pairCount;
	}
	
	public long getWord1Count() {
		return this.word1Count;
	}
	
	public long getWord2Count() {
		return this.word2Count;
	}
	
	public long getTotalNumberInDecade() {
		return this.totalNumberInDecade;
	}
	
	public double getPmi() {
		return this.pmi;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.pairCount = arg0.readLong();
		this.word1Count = arg0.readLong();
		this.word2Count = arg0.readLong();
		this.totalNumberInDecade = arg0.readLong();
		this.pmi = arg0.readLong();
		
	}
	

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeLong(this.pairCount);
		arg0.writeLong(this.word1Count);
		arg0.writeLong(this.word2Count);
		arg0.writeLong(this.totalNumberInDecade);
		arg0.writeDouble(this.pmi);
	}

	public String toString() {
		return  String.valueOf(pairCount) + " " + String.valueOf(word1Count) + " " + String.valueOf(word2Count) + " " + String.valueOf(totalNumberInDecade) + " " + String.valueOf(pmi);
	}
	
	@Override
	public DataStructureBase copy() {
		return new DSLayer6(this.pairCount, this.word1Count, this.word2Count, this.totalNumberInDecade, this.pmi);
	}	
}
