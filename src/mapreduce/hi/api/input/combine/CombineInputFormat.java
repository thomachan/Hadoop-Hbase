package mapreduce.hi.api.input.combine;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

import com.radiant.cisms.hdfs.seq.HInfoWritable;

/**
 * @author tom
 * Splits are constructed from the files under the input paths. 
 * A split cannot have files from different pools.
 * Each split returned may contain blocks from different files.
 * If a maxSplitSize is specified, then blocks on the same node are
 * combined to form a single split. Blocks that are left over are
 * then combined with other blocks in the same rack. 
 * If maxSplitSize is not specified, then blocks from the same rack
 * are combined in a single split; no attempt is made to create
 * node-local splits.
 * If the maxSplitSize is equal to the block size, then this class
 * is similar to the default splitting behavior in Hadoop: each
 * block is a locally processed split.
 * @see CombineSplit
 */

public class CombineInputFormat  extends CombineFileInputFormat<LongWritable, HInfoWritable> {  

  @Override
  public RecordReader<LongWritable, HInfoWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException {
		
		 String delimiter = context.getConfiguration().get( "inputformat.record.delimiter");
	    byte[] recordDelimiterBytes = null;
	    if (null != delimiter)
	      recordDelimiterBytes = delimiter.getBytes();
	    return (RecordReader<LongWritable, HInfoWritable>) new CombineRecordReader(recordDelimiterBytes,split,context);
	}
  
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
	createPool(new CustomPathFilter());
	return super.getSplits(job);
  }
 
}