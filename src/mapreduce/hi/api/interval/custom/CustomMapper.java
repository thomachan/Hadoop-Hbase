package mapreduce.hi.api.interval.custom;

import java.io.IOException;

import mapreduce.hi.HIKey;
import mapreduce.hi.Intervals;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.radiant.cisms.hdfs.seq.HInfoWritable;

public class CustomMapper extends Mapper<LongWritable, HInfoWritable, HIKey, HInfoWritable> {
	//private HInfoWritable hiInfo = new HInfoWritable();
	private HIKey out = new HIKey();

	public void map(LongWritable key, HInfoWritable value, Context context)
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