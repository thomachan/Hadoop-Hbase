package mapreduce.hi.api.input.multifile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mapreduce.hi.api.Notused;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
@Notused(reason="concept failed !")
public class MultiFileSplit extends InputSplit implements Writable {
	  private List<FileSplit> files;
	  private List<String> hosts;

	  public MultiFileSplit() {}

	  /** Constructs a split with host information
	   *
	   * @param files the file names
	   * @param start the position of the first byte in the file to process
	   * @param length the number of bytes in the file to process
	   * @param hosts the list of hosts containing the block, possibly null
	   */
	  public MultiFileSplit(List<FileSplit> files) {
	    this.files = files;
	  }

	  
	  public MultiFileSplit(FileSplit makeSplit) {
		  List<FileSplit> files = new ArrayList<FileSplit>();
		  files.add(makeSplit);
	}

	/** The number of bytes in the file to process. */
	  @Override
	  public long getLength() {
		  long length = 0;
		  for(FileSplit split: files){
			  length+=split.getLength();
		   }
		  return length; 
		}

	  ////////////////////////////////////////////
	  /////////	 Writable methods	//////////////
	  ////////////////////////////////////////////

	  @Override
	  public void write(DataOutput out) throws IOException {
		   for(FileSplit split: files){
			   split.write(out);
		   }
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
		  for(FileSplit split: files){
			   split.readFields(in);
		   }
	  }

	  @Override
	  public String[] getLocations() throws IOException {
		  hosts = new ArrayList<String>();
		  for(FileSplit split: files){
			  hosts.addAll(Arrays.asList(split.getLocations()));
		   }
		return hosts.toArray(new String[0]);
	  }

		public void add(FileSplit split) {
			if(files == null){
				files = new ArrayList<FileSplit>();
			}
			files.add(split);
		}

		public FileSplit get(int i) {
			return files!=null ? files.get(i):null;
		}
	}
