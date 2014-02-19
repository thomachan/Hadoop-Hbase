package mapreduce.hi.api.interval.custom;

import java.io.IOException;

import mapreduce.hi.HIKey;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.radiant.cisms.hdfs.seq.HInfoWritable;

public class CustomReducer extends Reducer<HIKey, HInfoWritable, HIKey, HInfoWritable> {
	private HInfoWritable result = null;
	private MultipleOutputs<HIKey, HInfoWritable> out;
	 
	 public void setup(Context context) {
	   out = new MultipleOutputs<HIKey,HInfoWritable>(context);	  
	 }
	public void reduce(HIKey key, Iterable<HInfoWritable> values, Context context)
			throws IOException, InterruptedException {
		result = null;
		double sum = 0;
		int count = 0;
		for (HInfoWritable val : values) {
			if (result == null) {
				result = new HInfoWritable();
			}
			sum += Double.valueOf(val.getValue());
			count += val.getCount();
		}
		result.setCount(count);
		result.setValue(sum / count);
		result.setTime(key.getTime());
		result.setOid(key.getOid().toString());
		result.setObjId(key.getObjId().get());
		//context.write(key, result);
		out.write(key, result,"hi/"+result.getObjId());
	}
	public void cleanup(Context context) throws IOException,InterruptedException {
		out.close();		
	 }
}
