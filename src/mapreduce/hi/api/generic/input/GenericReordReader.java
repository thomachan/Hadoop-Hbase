package mapreduce.hi.api.generic.input;

import java.io.IOException;

import mapreduce.hi.api.generic.io.BufferedValueWritable;
import mapreduce.hi.api.generic.io.KeyWritable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
/**
 * 
 * @author tom
 *
 * This will read record as Key (K) and Value(V) from the input path / file(s) 
 * provided by GenericInputFormat. It uses a array of delimiter bytes provided by 
 * the same  GenericInputFormat to resolve each record from the stream of bytes.
 * It process only single path at a time. Internally it uses a helper class for 
 * basic read operations. 
 * 
 * @see GenericReader 
 * 
 * @param <K>
 * @param <V>
 */
public abstract class GenericReordReader<K extends KeyWritable<K>,V extends BufferedValueWritable<V>> extends RecordReader<K,V>{
	 private static final Log LOG = LogFactory.getLog(GenericReordReader.class);
	  public static final String MAX_LINE_LENGTH = 
	    "mapreduce.input.linerecordreader.line.maxlength";
	  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
	  private long start;
	  private long pos;
	  private long end;
	  private GenericReader<V> in;
	  private FSDataInputStream fileIn;
	  private int maxLineLength;
	  private K key;
	  private V value;
	  private Decompressor decompressor;
	  private byte[] recordDelimiterBytes;
	  
	public GenericReordReader(byte[] recordDelimiterBytes) {
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
	public K getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	@Override
	public V getCurrentValue() throws IOException, InterruptedException {
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

	    FileSplit split = (FileSplit) genericSplit;
	    Configuration job = context.getConfiguration();
	    this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
	    start = split.getStart();
	    end = start + split.getLength();
	    final Path file = split.getPath();

	    // open the file and seek to the start of the split
	    final FileSystem fs = file.getFileSystem(job);
	    fileIn = fs.open(file);
	    
	    CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
	    boolean skipFirstLine = false;
	    if (codec != null) {
	      this.in = new GenericReader<V>(codec.createInputStream(fileIn), job,recordDelimiterBytes);
	      this.end = 9223372036854775807L;
	    } else {
	      if (this.start != 0L) {
	        skipFirstLine = true;
	        this.start -= 1L;
	        fileIn.seek(this.start);
	      }
	      this.in = new GenericReader<V>(fileIn, job,recordDelimiterBytes);
	    }
	    if (skipFirstLine) {
	      this.start += this.in.readObject(getNewValue(DEFAULT_BUFFER_SIZE), 0, (int)Math.min(2147483647L, this.end - this.start));
	    }

	    this.pos = this.start;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

	    if (this.key == null) {
	      this.key = getKey();
	    }
	    this.key.setBytesRead(this.pos);
	    if (this.value == null) {
	      this.value = getNewValue(DEFAULT_BUFFER_SIZE);
	    }
	    int newSize = 0;
	    while (this.pos < this.end) {
	      newSize = this.in.readObject(this.value, this.maxLineLength, Math.max((int)Math.min(2147483647L, this.end - this.pos), this.maxLineLength));

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

	    if (newSize == 0) {
	      this.key = null;
	      this.value = null;
	      return false;
	    }
	    return true;
	}

	protected abstract V getNewValue(int defaultBufferSize);

	protected abstract K getKey();

}
