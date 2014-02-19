package mapreduce.hi.api.output.defaultcustom;

import java.io.DataOutputStream;

import mapreduce.hi.HIKey;
import mapreduce.hi.api.generic.output.GenericRecordWriter;
import mapreduce.hi.api.input.defaultcustom.Value;

public class DefaultRecordWriter extends GenericRecordWriter<HIKey, Value>{

	public DefaultRecordWriter(DataOutputStream out, byte[] recordDelimiter,
			String keyValueSeparator) {
		super(out, recordDelimiter, keyValueSeparator);
	}

}
