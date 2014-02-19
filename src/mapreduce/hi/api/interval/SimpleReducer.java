package mapreduce.hi.api.interval;

import java.io.IOException;

import mapreduce.hi.HIKey;
import mapreduce.hi.HITuple;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SimpleReducer extends Reducer<HIKey, HITuple, HIKey, HITuple> {
	private HITuple result = null;
	private MultipleOutputs<HIKey,HITuple> out;
	 
	 public void setup(Context context) {
	   out = new MultipleOutputs<HIKey,HITuple>(context);	  
	 }
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
		out.write(key, result,"hi/"+result.getObjId().toString());
	}
	public void cleanup(Context context) throws IOException,InterruptedException {
		out.close();		
	 }
}
