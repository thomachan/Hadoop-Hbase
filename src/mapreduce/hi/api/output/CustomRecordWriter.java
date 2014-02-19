package mapreduce.hi.api.output;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import mapreduce.hi.HIKey;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.radiant.cisms.hdfs.seq.HInfoWritable;

public class CustomRecordWriter extends RecordWriter<HIKey, HInfoWritable> {

    private byte[] keyValueSeparator;
	private DataOutputStream out;
	private byte[] recordDelimiter;
    private static final String utf8 = "UTF-8";
	public CustomRecordWriter(DataOutputStream out, String keyValueSeparator) {
      this.out = out;
      try {
        this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
      } catch (UnsupportedEncodingException e) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }
	public CustomRecordWriter(DataOutputStream out, byte[] recordDelimiter, String keyValueSeparator) {
	      this.out = out;
	      try {
	        this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
	      } catch (UnsupportedEncodingException e) {
	        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
	      }
	      this.recordDelimiter = recordDelimiter;
	  }
	public CustomRecordWriter(DataOutputStream out, byte[] recordDelimiter) {
	      this.out = out;	      
	      this.recordDelimiter = recordDelimiter;
	 }
	@Override
	public synchronized void close(TaskAttemptContext context) throws IOException {
      out.close();
    }

	@Override
	public synchronized void write(HIKey key, HInfoWritable value) throws IOException,InterruptedException {
		value.write(out);
		out.write(recordDelimiter);
		
	} 
	
}
