package mapreduce.hi.api.generic.output;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import mapreduce.hi.api.generic.io.BufferedValueWritable;
import mapreduce.hi.api.generic.io.KeyWritable;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class GenericRecordWriter<K extends KeyWritable<K>,V extends BufferedValueWritable<V>> extends RecordWriter<K, V>{
	protected byte[] keyValueSeparator;
	protected DataOutputStream out;
	protected byte[] recordDelimiter;	
	protected static final String utf8 = "UTF-8";
	
	public GenericRecordWriter(DataOutputStream out, String keyValueSeparator) {
      this.out = out;
      try {
        this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
      } catch (UnsupportedEncodingException e) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }
	public GenericRecordWriter(DataOutputStream out, byte[] recordDelimiter, String keyValueSeparator) {
	      this.out = out;
	      try {
	        this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
	      } catch (UnsupportedEncodingException e) {
	        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
	      }
	      this.recordDelimiter = recordDelimiter;
	  }
	public GenericRecordWriter(DataOutputStream out, byte[] recordDelimiter) {
	      this.out = out;	      
	      this.recordDelimiter = recordDelimiter;
	 }
	
	@Override
	public void close(TaskAttemptContext arg0) throws IOException,
			InterruptedException {
		 out.close();		
	}

	@Override
	public void write(K k, V v) throws IOException, InterruptedException {
		v.write(out);
		out.write(recordDelimiter);
	}

}
