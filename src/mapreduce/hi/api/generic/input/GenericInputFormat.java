package mapreduce.hi.api.generic.input;

import java.io.IOException;

import mapreduce.hi.api.MRJobConfig;
import mapreduce.hi.api.generic.io.BufferedValueWritable;
import mapreduce.hi.api.generic.io.KeyWritable;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
/**
 * 
 * @author tom
 * This is a generic form of input format for K & V.
 * It provide the input <K,V> pair for Map task.
 * It uses a GenericReordReader, which can read each record as 'V' those are delimited by 
 * the given 'MRJobConfig.MR_RECORD_DELIMITER' from HDFS for a given input path
 * 
 * @see GenericReordReader
 * 
 * @param <K>
 * @param <V>
 */
public abstract class GenericInputFormat<K extends KeyWritable<K>,V extends BufferedValueWritable<V>> extends FileInputFormat<K, V>{

	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		
		 String delimiter = context.getConfiguration().get( MRJobConfig.MR_RECORD_DELIMITER);
	    byte[] recordDelimiterBytes = null;
	    if (null != delimiter)
	      recordDelimiterBytes = delimiter.getBytes();
	    return getRecordReader(recordDelimiterBytes);
	}

	protected abstract RecordReader<K, V> getRecordReader(byte[] recordDelimiterBytes);

}
