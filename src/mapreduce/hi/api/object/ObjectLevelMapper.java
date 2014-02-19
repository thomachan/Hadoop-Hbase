package mapreduce.hi.api.object;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ObjectLevelMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	private LongWritable out = new LongWritable();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//read a single line stroed in hdfs
		String txt = value.toString();
		if (txt == null) {
			return;
		}		
		StringTokenizer itr = new StringTokenizer(txt,"<=>");		
		if (itr.hasMoreTokens()) {			
			out.set(Long.valueOf(itr.nextToken()));	
			context.write(out, value);
		}
	}
}