package mapreduce.hi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import mapreduce.hi.api.generic.io.KeyWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

public class HIKey extends KeyWritable<HIKey>{
	private Long time;
	private Text oid;
	private LongWritable objId;
	private String interval;
	private SimpleDateFormat sfrmt =  new SimpleDateFormat("dd-MMM-yyyy hh:mm:ss a");
	
	public HIKey(){
		oid = new Text();
		objId = new LongWritable();
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		time = in.readLong();
		oid.readFields(in);
		objId.readFields(in);
		interval = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(time);
		oid.write(out);
		objId.write(out);
		WritableUtils.writeString(out, interval);
	}

	@Override
	public int compareTo(HIKey o) {
		int rtn=0;
		if((rtn = this.objId.compareTo(o.objId)) == 0 ){
			if((rtn = this.oid.compareTo(o.oid)) == 0){
				rtn = this.time.compareTo(o.time);
			}			
		}
		return rtn;
	}

	@Override
	public String toString() {
		
		return oid + "<>" + sfrmt.format(new Date(time));
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public Text getOid() {
		return oid;
	}

	public void setOid(Text oid) {
		this.oid = oid;
	}
	public String getInterval() {
		return interval;
	}
	public void setInterval(String interval) {
		this.interval = interval;
	}
	public LongWritable getObjId() {
		return objId;
	}
	public void setObjId(LongWritable objId) {
		this.objId = objId;
	}
}
