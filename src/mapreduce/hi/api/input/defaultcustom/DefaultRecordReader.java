package mapreduce.hi.api.input.defaultcustom;

import mapreduce.hi.api.generic.input.GenericReordReader;

public class DefaultRecordReader extends GenericReordReader<Key, Value>{

	public DefaultRecordReader(byte[] recordDelimiterBytes) {
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
