package mapreduce.hi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mapreduce.hi.api.Notused;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
public class HITuple implements Writable {
	private Long objId;
	private Long time;
	private Text value;
	private long count = 0;
	private Text oid;
	private Text interval;
	private Double avg;
	//private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	public HITuple(){
		oid = new Text();
		value = new Text();
		interval = new Text();
	}
	public Long getTime() {
		return time;
	}

	public Long getObjId() {
		return objId;
	}

	public void setObjId(Long objId) {
		this.objId = objId;
	}

	public void setTime(Long time) {
		this.time = time;
	}
	public Text getValue() {
		return value;
	}

	public void setValue(Text value) {
		this.value = value;
	}

	public Text getOid() {
		return oid;
	}

	public void setOid(Text oid) {
		this.oid = oid;
	}

	public Text getInterval() {
		return interval;
	}

	public void setInterval(Text interval) {
		this.interval = interval;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public Double getAvg() {
		return avg;
	}
	public void setAvg(Double avg) {
		this.avg = avg;
	}
	public void readFields(DataInput in) throws IOException {
		// Read the data out in the order it is written,
		// creating new Date objects from the UNIX timestamp
		objId = in.readLong();
		oid.readFields(in);
		value.readFields(in);
		time = in.readLong();
		interval.readFields(in);
		count = in.readLong();
		//avg = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		// Write the data out in the order it is read,
		// using the UNIX timestamp to represent the Date
		out.writeLong(objId);
		oid.write(out);
		value.write(out);
		out.writeLong(time);
		interval.write(out);
		out.writeLong(count);
		//out.writeDouble(avg);
	}

	public String toString() {
		return objId +"<=>" + (avg != null? avg: value) + "<=>" + count + "<=>" + interval;
	}
}