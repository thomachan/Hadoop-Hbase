package mapreduce.hi.api.interval.db;

import java.io.IOException;

import mapreduce.hi.Intervals;
import mapreduce.hi.api.output.db.HIKeyDBWritable;
import mapreduce.hi.api.output.db.HInfoDBWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.radiant.cisms.hdfs.seq.HInfoWritable;

public class DBMapper extends Mapper<LongWritable, HInfoWritable, HIKeyDBWritable, HInfoDBWritable> {
	//private HInfoWritable hiInfo = new HInfoWritable();
	private HIKeyDBWritable out = new HIKeyDBWritable();
	private HInfoDBWritable result = new HInfoDBWritable();

	public void map(LongWritable key, HInfoWritable value, Context context)
			throws IOException, InterruptedException {
				result.setCount(1);
				result.setObjId(value.getObjId());
				result.setOid(value.getOid());
				result.setValue(value.getValue());
				
				// create key [ oid: time ]
				long time = truncate(value.getTime(),Intervals.HOUR);
				out.setOid(new Text(value.getOid()));
				out.setObjId(new LongWritable(value.getObjId()));
				out.setTime(time);
				out.setInterval(Intervals.HOUR.name());
				//no need to hold original time
				result.setTime(time);
				
				context.write(out, result);
			}

	private long truncate(Long time, Intervals inr) {
		return inr.getTime(time);
	}
}