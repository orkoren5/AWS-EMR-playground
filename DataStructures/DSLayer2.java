package DataStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DSLayer2 extends DataStructureBase  {
	
	String word;
	long number;			
	
	public DSLayer2() {
		
	}
	
	public DSLayer2(String word, long number) {
		this.word = word;
		this.number = number;
	}
	
	public long getNumber() {
		return this.number;
	}
	
	public String getWord() {
		return this.word.equals(EMPTY_STR) ? "" : this.word;
	}
	
	public boolean isWordEmpty() {
		return this.word.equals(EMPTY_STR);
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.word = arg0.readUTF();
		this.number = arg0.readLong();
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(this.word);
		arg0.writeLong(this.number);
	}
	
	@Override
	public String toString() {
		return this.word + " " + this.number;
	}

	@Override
	public DataStructureBase copy() {		
		return new DSLayer2(this.word, this.number);
	}
}
