package mapreduce.hi.api.object;

import java.io.IOException;

import mapreduce.hi.HITuple;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ObjectLevelReducer extends Reducer<LongWritable, Text, LongWritable, HITuple> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
	}
}
