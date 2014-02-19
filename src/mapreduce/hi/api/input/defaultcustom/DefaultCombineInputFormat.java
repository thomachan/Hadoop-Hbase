package mapreduce.hi.api.input.defaultcustom;

import mapreduce.hi.api.generic.input.GenericCombineInputFormat;

import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class DefaultCombineInputFormat extends GenericCombineInputFormat<Key, Value>{

	

	@Override
	protected RecordReader<Key, Value> getRecordReader(
			byte[] recordDelimiterBytes, InputSplit split,
			TaskAttemptContext context) {
		return new DefaultCombineRecordReader(recordDelimiterBytes);
	}

	@Override
	protected PathFilter[] pathFilter() {
		return new PathFilter[]{new DefaultPathFilter()};
	}

}
