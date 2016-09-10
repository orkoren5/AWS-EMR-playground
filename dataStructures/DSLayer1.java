package DataStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DSLayer1 extends DataStructureBase  {
	
	long number;			
	
	public DSLayer1(long number) {			
		this.number = number;
	}
	
	public long getNumber() {
		return number;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {			
		this.number = arg0.readLong();
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {			
		arg0.writeLong(this.number);
	}	
}
