package mapreduce.hi.api.interval;

import java.io.IOException;
import java.util.StringTokenizer;

import mapreduce.hi.HIKey;
import mapreduce.hi.HITuple;
import mapreduce.hi.Intervals;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimpleMapper extends Mapper<LongWritable, Text, HIKey, HITuple> {
	private HITuple hiTuple = new HITuple();
	private HIKey out = new HIKey();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {String txt = value.toString();
			// .get will return null if the key is not there
			if (txt == null) {
				// skip this record
				return;
			}
			// Tokenize the string by splitting it up on whitespace into
			// something we can iterate over,
			// then send the tokens away
			StringTokenizer itr = new StringTokenizer(txt,"||");
			
			while (itr.hasMoreTokens()) {
				//read a single line stroed in hdfs
				hiTuple.setObjId(Long.valueOf(itr.nextToken()));
				hiTuple.setOid(new Text(itr.nextToken()));
				hiTuple.setValue(new Text(itr.nextToken()));
				hiTuple.setTime(Long.valueOf(itr.nextToken()));
				hiTuple.setInterval(new Text(Intervals.HOUR.toString()));
				hiTuple.setCount(1);
				
				// create key [ oid: time ]
				long time = truncate(hiTuple.getTime(),Intervals.HOUR);
				out.setOid(hiTuple.getOid());
				out.setTime(time);
				context.write(out, hiTuple);
			}}

	private long truncate(Long time, Intervals inr) {
		return inr.getTime(time);
	}
}