package mapreduce.hi.api.input.defaultcustom;

import mapreduce.hi.api.generic.input.GenericInputFormat;

import org.apache.hadoop.mapreduce.RecordReader;

public class DefaultInputFormat extends GenericInputFormat<Key, Value>{

	@Override
	protected RecordReader<Key, Value> getRecordReader(
			byte[] recordDelimiterBytes) {		
		return new DefaultRecordReader(recordDelimiterBytes);
	}

}
