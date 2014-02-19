package mapreduce.hi.api.input.defaultcustom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mapreduce.hi.api.generic.io.KeyWritable;

public class Key extends KeyWritable<Key>{
	
	@Override
	public void readFields(DataInput in) throws IOException {
		bytesRead = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(bytesRead);
	}

	@Override
	public int compareTo(Key o) {
		return this.bytesRead > o.bytesRead ?1:(this.bytesRead == o.bytesRead ? 0:-1);
	}

}
