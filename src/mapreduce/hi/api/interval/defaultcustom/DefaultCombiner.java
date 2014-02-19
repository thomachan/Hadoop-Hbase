package mapreduce.hi.api.interval.defaultcustom;

import java.io.IOException;

import mapreduce.hi.HIKey;
import mapreduce.hi.api.input.defaultcustom.Value;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class DefaultCombiner extends Reducer<HIKey, Value, HIKey, Value> {
	private Value result = null;
	private MultipleOutputs<HIKey, Value> out;
	 
	 public void setup(Context context) {
	   out = new MultipleOutputs<HIKey,Value>(context);	  
	 }
	public void reduce(HIKey key, Iterable<Value> values, Context context)
			throws IOException, InterruptedException {
		result = null;
		double sum = 0;
		int count = 0;
		for (Value val : values) {
			if (result == null) {
				result = new Value();
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
		//out.write(key, result,"hi/"+result.getObjId());
	}
	public void cleanup(Context context) throws IOException,InterruptedException {
		out.close();		
	 }
}
