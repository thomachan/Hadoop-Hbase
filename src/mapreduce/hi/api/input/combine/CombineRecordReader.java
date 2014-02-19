package mapreduce.hi.api.input.combine;

import java.io.IOException;

import mapreduce.hi.api.input.CustomReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import com.radiant.cisms.hdfs.seq.HInfoWritable;

public class CombineRecordReader extends RecordReader<LongWritable, HInfoWritable>{
	 private static final Log LOG = LogFactory.getLog(CombineRecordReader.class);
	  public static final String MAX_LINE_LENGTH = 
	    "mapreduce.input.linerecordreader.line.maxlength";
	  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
	  private long start;
	  private long pos;
	  private long end;
	  private int index;
	  private CustomReader in;
	  private FSDataInputStream fileIn;
	  private int maxLineLength;
	  private int maxIndex;
	  private LongWritable key;
	  private HInfoWritable value;
	  private Decompressor decompressor;
	  private byte[] recordDelimiterBytes;
	  CombineFileSplit combineSplit;
	  TaskAttemptContext context;
	  private boolean isEnded;
	  
	  // for test/debug purpose
	  private int[] numRecords = null;
	  private int keyTracker;
	  private int initTracker;
	  private int configTracker;
	  
	public CombineRecordReader(byte[] recordDelimiterBytes) {
		this.recordDelimiterBytes = recordDelimiterBytes;
	}
	public CombineRecordReader(byte[] recordDelimiterBytes,InputSplit split,
            TaskAttemptContext context) {
		this.recordDelimiterBytes = recordDelimiterBytes;
	}

	@Override
	public synchronized void close() throws IOException {
	    try {
	      if (in != null) {
	        in.close();
	      }
	    } finally {
	      if (decompressor != null) {
	        CodecPool.returnDecompressor(decompressor);
	      }
	    }
	  }

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	@Override
	public HInfoWritable getCurrentValue() throws IOException, InterruptedException {
		this.value.read();
		this.value.clear();
		return this.value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (this.start == this.end) {
		      return 0.0F;
		    }
		    return Math.min(1.0F, (float)(this.pos - this.start) / (float)(this.end - this.start));
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		combineSplit = (CombineFileSplit) genericSplit;
		this.context = context;
		maxIndex = combineSplit.getNumPaths();
		initTracker++;
	    configureFileSplit();
	}
	/**
	 * this method will initialize and configure reader for next immediate file split 
	 * in the Combine split.  
	 * @throws IOException
	 */
	private void configureFileSplit()
			throws IOException {
		configTracker++;
		if(index >= maxIndex){
			isEnded= true;
		}else{
			/*if(index >= maxIndex-1){
				throw new NotFoundException(getMessage());
			}
			
			if(numRecords == null || numRecords.length == 0){
				numRecords =  new int[combineSplit.getMaxIndex()];
			}*/
			isEnded= false;
			//throw new NotFoundException(combineSplit.toString());
		}
		if(!isEnded){
			    Configuration job = context.getConfiguration();
			    this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
			    start = combineSplit.getOffset(index);
			    end = start + combineSplit.getLength(index);
			    //if file has no content
			    if(start >= end){
			    	index++;
			    	configureFileSplit();
			    }
			    final Path file = combineSplit.getPath(index);
		
			    // open the file and seek to the start of the split
			    final FileSystem fs = file.getFileSystem(job);
			    fileIn = fs.open(file);
			    
			    CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
			    boolean skipFirstLine = false;
			    if (codec != null) {
			      this.in = new CustomReader(codec.createInputStream(fileIn), job,recordDelimiterBytes);
			      this.end = 9223372036854775807L;
			    } else {
			      if (this.start != 0L) {
			        skipFirstLine = true;
			        this.start -= 1L;
			        fileIn.seek(this.start);
			        
			      }
			      this.in = new CustomReader(fileIn, job,recordDelimiterBytes);
			    }
			    if (skipFirstLine) {
			      this.start += this.in.readObject(new HInfoWritable(DEFAULT_BUFFER_SIZE), 0, (int)Math.min(2147483647L, this.end - this.start));
			    }
		
			    this.pos = this.start;
			}
	}
	private String getMessage() {
		StringBuilder sb = new StringBuilder();
		for(int i=0;i< numRecords.length; i++){
			sb.append("\nPath["+i+"] : "+numRecords[i]);
		}
		for(int i=0;i< numRecords.length; i++){
			sb.append("\nLength["+i+"] : "+combineSplit.getLength(i)+"\tOffset["+i+"] : "+combineSplit.getOffset(i));
		}
		sb.append("\nIndex :"+ index);
		sb.append("\nkeyTracker :"+ keyTracker);
		sb.append("\ninitTracker :"+ initTracker);
		sb.append("\nconfigTracker :"+ configTracker);
		return sb.toString();
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		/*keyTracker++;
		if(isEnded){
    		throw new NotFoundException(getMessage());
    	}*/
		 if(isEnded){
	    	  this.key = null;
		      this.value = null;
		      return false;
	    }
		 
	    if (this.key == null) {
	      this.key = new LongWritable();
	    }
	    this.key.set(this.pos);
	    if (this.value == null) {
	      this.value = new HInfoWritable(DEFAULT_BUFFER_SIZE);
	    }
	    int newSize = 0;
	    while (this.pos < this.end) {
	      newSize = this.in.readObject(this.value, this.maxLineLength, Math.max((int)Math.min(2147483647L, this.end - this.pos), this.maxLineLength));
    	 // this.numRecords[index] +=newSize ;
	      if (newSize == 0) {
	        break;
	      }
	      this.pos += newSize;
	      if (newSize < this.maxLineLength)
	      {
	        break;
	      }

	      LOG.info("Skipped line of size " + newSize + " at pos " + (this.pos - newSize));
	    }
	    
	    if(this.pos >= this.end || newSize == 0){// if one flie in the split is over, take next file
	    	index++;
	    	configureFileSplit();
	    }

	    return true;
	}

}
