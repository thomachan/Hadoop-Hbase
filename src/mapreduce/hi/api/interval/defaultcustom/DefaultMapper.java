package mapreduce.hi.api.interval.defaultcustom;

import java.io.IOException;

import mapreduce.hi.HIKey;
import mapreduce.hi.Intervals;
import mapreduce.hi.api.input.defaultcustom.Key;
import mapreduce.hi.api.input.defaultcustom.Value;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DefaultMapper extends Mapper<Key, Value, HIKey, Value> {
	//private HInfoWritable hiInfo = new HInfoWritable();
	private HIKey out = new HIKey();

	public void map(Key key, Value value, Context context)
			throws IOException, InterruptedException {
				value.setCount(1);
				
				// create key [ oid: time ]
				long time = truncate(value.getTime(),Intervals.HOUR);
				out.setOid(new Text(value.getOid()));
				out.setObjId(new LongWritable(value.getObjId()));
				out.setTime(time);
				out.setInterval(Intervals.HOUR.name());
				//no need to hold original time
				value.setTime(time);
				
				context.write(out, value);
			}

	private long truncate(Long time, Intervals inr) {
		return inr.getTime(time);
	}
}