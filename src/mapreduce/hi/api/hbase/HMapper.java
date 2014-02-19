package mapreduce.hi.api.hbase;


import java.io.IOException;

import mapreduce.hi.HIKey;
import mapreduce.hi.Intervals;
import mapreduce.hi.api.input.defaultcustom.Value;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class HMapper extends TableMapper<HIKey, Value>  {
	

	private final Value val = new Value();
   	private HIKey key = new HIKey();

   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
        	long t = Bytes.toLong(value.getValue(HBASEConstants.CF_TIME, HBASEConstants.TIME_STAMP));
        	t= truncate(t, Intervals.HOUR);
        	key.setTime(t); 
        	key.setObjId(new LongWritable(Bytes.toLong(value.getValue(HBASEConstants.CF_INFO, HBASEConstants.OBJ_ID))));
        	key.setOid(new Text(Bytes.toString(value.getValue(HBASEConstants.CF_INFO, HBASEConstants.OID))));
        	
        	val.setValue(Bytes.toDouble(value.getValue(HBASEConstants.CF_INFO, HBASEConstants.VALUE)));
        	val.setCount(1);
        	context.write(key, val);
   	}
   	private long truncate(Long time, Intervals inr) {
		return inr.getTime(time);
	}
}