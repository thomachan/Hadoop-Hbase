package mapreduce.hi.api.output.defaultcustom;

import java.io.DataOutputStream;

import mapreduce.hi.HIKey;
import mapreduce.hi.api.generic.output.GenericOutputFormat;
import mapreduce.hi.api.input.defaultcustom.Value;

import org.apache.hadoop.mapreduce.RecordWriter;

public class DefaultOutputFormat extends GenericOutputFormat<HIKey, Value>{

	@Override
	protected RecordWriter<HIKey, Value> getRecordWriter(
			DataOutputStream fileOut, byte[] recordDelimiterBytes,
			String keyValueSeparator) {
		return new DefaultRecordWriter(fileOut,recordDelimiterBytes,keyValueSeparator);
	}

}
