package mapreduce.hi.api.input.defaultcustom;

import mapreduce.hi.api.generic.input.GenericCombineRecordReader;

public class DefaultCombineRecordReader extends GenericCombineRecordReader<Key, Value>{

	public DefaultCombineRecordReader(byte[] recordDelimiterBytes) {
		super(recordDelimiterBytes);
	}

	@Override
	protected Value getNewValue(int defaultBufferSize) {
		return new Value(defaultBufferSize);
	}

	@Override
	protected Key getKey() {
		return new Key();
	}

}
