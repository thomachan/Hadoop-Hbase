package mapreduce.hi.api.input.multifile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import mapreduce.hi.api.Notused;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.radiant.cisms.hdfs.seq.HInfoWritable;
/**
 * it is a dropped concept
 * @author tom
 *
 */
@Notused(reason="concept failed !")
public class MultiInputFormat extends
		FileInputFormat<LongWritable, HInfoWritable> {
	private static final Log LOG = LogFactory.getLog(FileInputFormat.class);
	public static final String NUM_INPUT_FILES = "mapreduce.input.fileinputformat.numinputfiles";
	private static final double SPLIT_SLOP = 1.1; // 10% slop
	private MultiFileSplit mSplit;

	@Override
	public RecordReader<LongWritable, HInfoWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {

		String delimiter = context.getConfiguration().get(
				"inputformat.record.delimiter");
		byte[] recordDelimiterBytes = null;
		if (null != delimiter)
			recordDelimiterBytes = delimiter.getBytes();
		return new MultiRecordReader(recordDelimiterBytes);
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		long maxSize = getMaxSplitSize(job);

		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);
		List<FileStatus> splitfiles = new ArrayList<FileStatus>();
		long fileslength = 0;
		long allowedlength = 0;
		int totalFiles = files != null ? files.size() : 0;

		if (totalFiles > 0) {
			for (int i = 0; i < totalFiles; i++) {
				FileStatus file = files.get(i);

				if (fileslength < maxSize) {// getting
					allowedlength = fileslength;
					splitfiles.add(file);
					fileslength += file.getLen();
				} else {					
					
					if (allowedlength != 0) {
						mSplit = new MultiFileSplit();
						perform(job, minSize, maxSize, splitfiles);
						
						splitfiles = new ArrayList<FileStatus>();
						fileslength = 0;
						splits.add(mSplit);
					}
				}
			}
			
			if (allowedlength != 0) {
				mSplit = new MultiFileSplit();
				perform(job, minSize, maxSize, splitfiles);
				splits.add(mSplit);
			}
		}
		// Save the number of input files for metrics/loadgen
		job.getConfiguration().setLong(NUM_INPUT_FILES, totalFiles);
		LOG.debug("Total # of splits: " + splits.size());
		return splits;
	}

	private void perform(JobContext job, long minSize, long maxSize,
			List<FileStatus> splitfiles) throws IOException {
		for (FileStatus tempFile : splitfiles) {
			Path path = tempFile.getPath();
			long length = tempFile.getLen();
			if (length != 0) {
				BlockLocation[] blkLocations;

				FileSystem fs = path.getFileSystem(job.getConfiguration());
				
				blkLocations = fs.getFileBlockLocations(tempFile, 0, length);

				if (isSplitable(job, path)) {
					long blockSize = tempFile.getBlockSize();
					long splitSize = computeSplitSize(
							blockSize, minSize, maxSize);

					long bytesRemaining = length;
					/*while (((double) bytesRemaining)/ splitSize > SPLIT_SLOP) {
						int blkIndex = getBlockIndex(blkLocations, length- bytesRemaining);
						splits.add(new MultiFileSplit(makeSplit(path, length- bytesRemaining, splitSize, blkLocations[blkIndex].getHosts())));
						bytesRemaining -= splitSize;
					}*/

					if (bytesRemaining != 0) {
						int blkIndex = getBlockIndex(blkLocations, length- bytesRemaining);
						mSplit.add(makeSplit(path, length- bytesRemaining,bytesRemaining,blkLocations[blkIndex].getHosts()));
					}
				} else { // not splitable
					mSplit.add(makeSplit(path, 0, length,blkLocations[0].getHosts()));
				}
			} else {
				// Create empty hosts array for zero length
				// files
				mSplit.add(makeSplit(path, 0, length,new String[0]));
			}
		}
	}

	protected FileSplit makeSplit(Path file, long start, long length,
			String[] hosts) {
		return new FileSplit(file, start, length, hosts);
	}
	
}
