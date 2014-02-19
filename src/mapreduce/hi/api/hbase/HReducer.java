package mapreduce.hi.api.hbase;


import java.io.IOException;

import mapreduce.hi.HIKey;
import mapreduce.hi.api.input.defaultcustom.Value;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class HReducer extends TableReducer<HIKey, Value, ImmutableBytesWritable>  {
	public static final byte[] CF_TIME = "time".getBytes();
	public static final byte[] TIME_STAMP = "timestamp".getBytes();
	private Value result = null;

 	public void reduce(HIKey key, Iterable<Value> values, Context context) throws IOException, InterruptedException {
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
    		
    		Put put = new Put(Bytes.toBytes(key.getTime()+"$"+key.getObjId()+"$"+key.getOid()));
			put.add(HBASEConstants.CF_TIME, HBASEConstants.TIME_STAMP,Bytes.toBytes(key.getTime()));
			put.add(HBASEConstants.CF_INFO,HBASEConstants.OBJ_ID,Bytes.toBytes(key.getObjId().get()));
			put.add(HBASEConstants.CF_INFO,HBASEConstants.OID,key.getOid().getBytes());
			put.add(HBASEConstants.CF_INFO,HBASEConstants.VALUE, Bytes.toBytes(sum));
			put.add(HBASEConstants.CF_INFO,HBASEConstants.VALUE, Bytes.toBytes(sum));
			put.add(HBASEConstants.CF_INFO,HBASEConstants.COUNT, Bytes.toBytes(count));
    		context.write(null, put);
   	}
}
