package mapreduce.hi.api.generic.output;

import java.io.DataOutputStream;
import java.io.IOException;

import mapreduce.hi.api.MRJobConfig;
import mapreduce.hi.api.generic.io.BufferedValueWritable;
import mapreduce.hi.api.generic.io.KeyWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public abstract class GenericOutputFormat<K extends KeyWritable<K>,V extends BufferedValueWritable<V>> extends FileOutputFormat<K, V>{

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {



	    Configuration conf = context.getConfiguration();
	    String delimiter = conf.get(MRJobConfig.MR_RECORD_DELIMITER);
	    byte[] recordDelimiterBytes = null;
	    if (null != delimiter){
	    	recordDelimiterBytes = delimiter.getBytes();
	    }else{
	    	recordDelimiterBytes = new byte[]{'$','$','$'};
	    }
	    boolean isCompressed = getCompressOutput(context);
	    String keyValueSeparator= conf.get(MRJobConfig.MR_KEY_VALUE_SEPARATOR, "\t");
	    CompressionCodec codec = null;
	    String extension = "";
	    if (isCompressed) {
	      Class<? extends CompressionCodec> codecClass = 
	        getOutputCompressorClass(context, GzipCodec.class);
	      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
	      extension = codec.getDefaultExtension();
	    }
	    Path file = getDefaultWorkFile(context, extension);
	    FileSystem fs = file.getFileSystem(conf);
	    if (!isCompressed) {
	      FSDataOutputStream fileOut = fs.create(file, false);
	      return getRecordWriter(fileOut,recordDelimiterBytes, keyValueSeparator);
	    } else {
	      FSDataOutputStream fileOut = fs.create(file, false);
	      return getRecordWriter(new DataOutputStream
	                                        (codec.createOutputStream(fileOut)),recordDelimiterBytes,
	                                        keyValueSeparator);
	    }
	
	
	}

	protected abstract RecordWriter<K, V> getRecordWriter(DataOutputStream fileOut,
			byte[] recordDelimiterBytes, String keyValueSeparator);

	
}
