package mapreduce.hi.api.input.defaultcustom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import mapreduce.hi.api.generic.io.BufferedValueWritable;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableUtils;

public class Value extends BufferedValueWritable<Value>{
	String oid;
	long objId;
	long time;
	double value;
	long count;
	private static final Log LOG = LogFactory.getLog(" org.apache.hadoop.io");
	private transient SimpleDateFormat sfrmt =  new SimpleDateFormat("dd-MMM hh:mma");
	public Value(){
		
	}
	public Value(int defaultBufferSize) {
		super(defaultBufferSize);
	}

	public Value(String oid, long objId, long time, double value) {
		this.oid = oid;
		this.objId = objId;
		this.time = time;
		this.value = value;

	}


	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, oid);
		out.writeLong(objId);
		out.writeLong(time);
		out.writeDouble(value);
		out.writeLong(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		try{
			oid = WritableUtils.readString(in);
			objId = in.readLong();
			time = in.readLong();
			value = in.readDouble();
			count = in.readLong();
		}catch(EOFException e){
//			LOG.info("End of File");
		}
	}

	@Override
	public int compareTo(Value val) {
		return CompareToBuilder.reflectionCompare(this, val);
	}
	@Override
	public String toString() {
		return objId + "\t" + sfrmt.format(new Date(time)) + "\t["+time+ "]\t" + Math.round(value)
				+ "\t" + count + "\t" + oid;
	}

	public String getOid() {
		return oid;
	}

	public void setOid(String oid) {
		this.oid = oid;
	}

	public long getObjId() {
		return objId;
	}

	public void setObjId(long objId) {
		this.objId = objId;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

}
