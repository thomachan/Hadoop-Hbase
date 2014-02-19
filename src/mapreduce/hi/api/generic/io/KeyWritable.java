package mapreduce.hi.api.generic.io;

import org.apache.hadoop.io.WritableComparable;
/**
 * @author tom
 * 
 * It is an abstract wrapper of Record Key K that is being read by RecordReader   
 * It only uses bytesRead	
 * @param <K>
 */
public abstract class KeyWritable<K> implements WritableComparable<K>{
	protected long bytesRead;

	public long getBytesRead() {
		return bytesRead;
	}

	public void setBytesRead(long bytesRead) {
		this.bytesRead = bytesRead;
	}
}
