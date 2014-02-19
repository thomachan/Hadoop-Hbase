package mapreduce.hi.api.interval.db;

import java.io.IOException;

import mapreduce.hi.api.output.db.HIKeyDBWritable;
import mapreduce.hi.api.output.db.HInfoDBWritable;

import org.apache.hadoop.mapreduce.Reducer;

public class DBCombiner extends Reducer<HIKeyDBWritable, HInfoDBWritable, HIKeyDBWritable, HInfoDBWritable> {
	private HInfoDBWritable result = null;
	 
	 public void setup(Context context) {
	 }
	public void reduce(HIKeyDBWritable key, Iterable<HInfoDBWritable> values, Context context)
			throws IOException, InterruptedException {
		result = null;
		double sum = 0;
		int count = 0;
		for (HInfoDBWritable val : values) {
			if (result == null) {
				result = new HInfoDBWritable();
			}
			sum += Double.valueOf(val.getValue());
			count += val.getCount();
		}
		result.setCount(count);
		result.setValue(sum / count);
		result.setTime(key.getTime());
		result.setOid(key.getOid().toString());
		result.setObjId(key.getObjId().get());
		context.write(key, result);
	}
	public void cleanup(Context context) throws IOException,InterruptedException {
	 }
}
