package mapreduce.hi.api.input.combine;

import java.io.IOException;
import java.lang.reflect.Constructor;

import mapreduce.hi.api.MRJobConfig;
import mapreduce.hi.api.Notused;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A generic RecordReader that can hand out different recordReaders
 * for each chunk in a {@link CombineSplit}.
 * A CombineSplit can combine data chunks from multiple files. 
 * This class allows using different RecordReaders for processing
 * these data chunks from different files.
 * @see CombineSplit
 */
@Notused(reason="not fully understood")
@InterfaceAudience.Public
@InterfaceStability.Stable
public class GenericCombineRecordReader<K, V> extends RecordReader<K, V> {

  static final Class [] constructorSignature = new Class [] 
                                         {CombineSplit.class,
                                          TaskAttemptContext.class,
                                          Integer.class};

private static final String MAP_INPUT_PATH = null;

private static final String MAP_INPUT_START = null;

private static final String MAP_INPUT_FILE = null;

  protected CombineSplit split;
  protected Class<? extends RecordReader<K,V>> rrClass;
  protected Constructor<? extends RecordReader<K,V>> rrConstructor;
  protected FileSystem fs;
  protected TaskAttemptContext context;
  
  protected int idx;
  protected long progress;
  protected RecordReader<K, V> curReader;
  
  public void initialize(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    this.split = (CombineSplit)split;
    this.context = context;
    if (null != this.curReader) {
      this.curReader.initialize(split, context);
    }
  }
  
  public boolean nextKeyValue() throws IOException, InterruptedException {

    while ((curReader == null) || !curReader.nextKeyValue()) {
      if (!initNextRecordReader()) {
        return false;
      }
    }
    return true;
  }

  public K getCurrentKey() throws IOException, InterruptedException {
    return curReader.getCurrentKey();
  }
  
  public V getCurrentValue() throws IOException, InterruptedException {
    return curReader.getCurrentValue();
  }
  
  public void close() throws IOException {
    if (curReader != null) {
      curReader.close();
      curReader = null;
    }
  }
  
  /**
   * return progress based on the amount of data processed so far.
   */
  public float getProgress() throws IOException, InterruptedException {
    long subprogress = 0;    // bytes processed in current split
    if (null != curReader) {
      // idx is always one past the current subsplit's true index.
      subprogress = (long)(curReader.getProgress() * split.getLength(idx - 1));
    }
    return Math.min(1.0f,  (progress + subprogress)/(float)(split.getLength()));
  }
  
  /**
   * A generic RecordReader that can hand out different recordReaders
   * for each chunk in the CombineSplit.
   */
  public GenericCombineRecordReader(CombineSplit split,
                                 TaskAttemptContext context,
                                 Class<? extends RecordReader<K,V>> rrClass)
    throws IOException {
    this.split = split;
    this.context = context;
    this.rrClass = rrClass;
    this.idx = 0;
    this.curReader = null;
    this.progress = 0;

    try {
      rrConstructor = rrClass.getDeclaredConstructor(constructorSignature);
      rrConstructor.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(rrClass.getName() + 
                                 " does not have valid constructor", e);
    }
    initNextRecordReader();
  }
  
  /**
   * Get the record reader for the next chunk in this CombineSplit.
   */
  protected boolean initNextRecordReader() throws IOException {

    if (curReader != null) {
      curReader.close();
      curReader = null;
      if (idx > 0) {
        progress += split.getLength(idx-1);    // done processing so far
      }
    }

    // if all chunks have been processed, nothing more to do.
    if (idx == split.getNumPaths()) {
      return false;
    }

    // get a record reader for the idx-th chunk
    try {
      Configuration conf = context.getConfiguration();
      // setup some helper config variables.
      conf.set(MRJobConfig.MAP_INPUT_FILE, split.getPath(idx).toString());
      conf.setLong(MRJobConfig.MAP_INPUT_START, split.getOffset(idx));
      conf.setLong(MRJobConfig.MAP_INPUT_PATH, split.getLength(idx));

      curReader =  rrConstructor.newInstance(new Object [] 
                            {split, context, Integer.valueOf(idx)});

      if (idx > 0) {
        // initialize() for the first RecordReader will be called by MapTask;
        // we're responsible for initializing subsequent RecordReaders.
        curReader.initialize(split, context);
      }
    } catch (Exception e) {
      throw new RuntimeException (e);
    }
    idx++;
    return true;
  }
}