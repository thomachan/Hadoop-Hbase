package mapreduce.hi.api.hbase;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import mapreduce.hi.HIKey;
import mapreduce.hi.api.Configurator;
import mapreduce.hi.api.input.defaultcustom.Value;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

public class HBaseConfigurator implements Configurator{

	private static SimpleDateFormat sfrmt =  new SimpleDateFormat("dd-MMM-yyyy hh:mma");

	@Override
	public Job getJob(Configuration conf) throws IOException {
		Job job = new Job(conf,"HOURLY_AVG");
		job.setJarByClass(HBaseJobRunner.class);     // class that contains mapper and reducer

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		
		try {
			String startTime = Long.toString(sfrmt.parse("05-Feb-2014 03:00PM").getTime());
			//String endTime = Long.toString(sfrmt.parse("05-Feb-2014 10:07AM").getTime());
			 
			 scan.setStartRow(Bytes.toBytes(startTime));
			// scan.setStopRow(Bytes.toBytes(endTime));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// set other scan attrs
		TableMapReduceUtil.initTableMapperJob(
			HBASEConstants.TABLE_HINFO,        // input table
			scan,               // Scan instance to control CF and attribute selection
			HMapper.class,     // mapper class
			HIKey.class,         // mapper output key
			Value.class,  // mapper output value
			job);
		TableMapReduceUtil.initTableReducerJob(
			HBASEConstants.TABLE_HOUR_INFO,        // output table
			HReducer.class,    // reducer class
			job);
		job.setNumReduceTasks(1);   // at least one, adjust as required
		return job;
	}

	@Override
	public Job getJob(Configuration conf, String[] otherArgs)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
