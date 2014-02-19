package mapreduce.hi.seq;

import java.io.IOException;

import mapreduce.hi.HIKey;
import mapreduce.hi.HITuple;
import mapreduce.hi.Intervals;
import mapreduce.hi.api.Notused;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.radiant.cisms.hdfs.seq.HInfoWritable;
@Notused(reason="concept failed !")
public class SeqMapReduce {
	public static class Map extends Mapper<Text , HInfoWritable , HIKey, HITuple> {
		private HIKey out = new HIKey();
		private HITuple hiTuple = new HITuple();

		public void map(Text  key, HInfoWritable  value, Context context)
				throws IOException, InterruptedException {// Parse the input
															// string into a
			
			// .get will return null if the key is not there
			if (value == null) {
				// skip this record
				return;
			}
			// Tokenize the string by splitting it up on whitespace into
			// something we can iterate over,
			// then send the tokens away

				// read a single line stroed in hdfs
				hiTuple.setObjId(Long.valueOf(value.getObjId()));
				hiTuple.setOid(new Text(value.getOid()));
				hiTuple.setValue(new Text(new Double(value.getValue()).toString()));
				hiTuple.setTime(value.getTime());
				hiTuple.setInterval(new Text(Intervals.HOUR.toString()));
				hiTuple.setCount(1);

				// create key [ oid: time ]
				long time = truncate(hiTuple.getTime(), Intervals.HOUR);
				out.setOid(hiTuple.getOid());
				out.setTime(time);
				context.write(out, hiTuple);
		}

		private long truncate(Long time, Intervals inr) {
			return inr.getTime(time);
		}
	}

	public static class Reduce extends Reducer<HIKey, HITuple, HIKey, HITuple> {
		private HITuple result = null;

		public void reduce(HIKey key, Iterable<HITuple> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			for (HITuple val : values) {
				if (result == null) {
					result = val;
				}
				sum += Double.valueOf(val.getValue().toString());
				count += val.getCount();
			}
			result.setCount(count);
			result.setAvg(sum / count);
			context.write(key, result);
		}
	}
}
