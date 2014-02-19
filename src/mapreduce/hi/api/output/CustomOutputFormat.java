package mapreduce.hi.api.output;

import java.io.DataOutputStream;
import java.io.IOException;

import mapreduce.hi.HIKey;
import mapreduce.hi.HITuple;

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

import com.radiant.cisms.hdfs.seq.HInfoWritable;

public class CustomOutputFormat extends FileOutputFormat<HIKey, HInfoWritable>  {

	private static final String SEPERATOR = null;

	@Override
	public RecordWriter<HIKey, HInfoWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {

	    Configuration conf = context.getConfiguration();
	    String delimiter = conf.get("mapreduce.jdbc.driver.class");
	    byte[] recordDelimiterBytes = null;
	    if (null != delimiter){
	    	recordDelimiterBytes = delimiter.getBytes();
	    }else{
	    	recordDelimiterBytes = new byte[]{'$','$','$'};
	    }
	    boolean isCompressed = getCompressOutput(context);
	    String keyValueSeparator= "\t";//conf.get(SEPERATOR, "\t");
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
	      return new CustomRecordWriter(fileOut,recordDelimiterBytes, keyValueSeparator);
	    } else {
	      FSDataOutputStream fileOut = fs.create(file, false);
	      return new CustomRecordWriter(new DataOutputStream
	                                        (codec.createOutputStream(fileOut)),recordDelimiterBytes,
	                                        keyValueSeparator);
	    }
	}

}
