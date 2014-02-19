package mapreduce.hi.api.interval.db;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.radiant.cisms.hdfs.seq.HInfoWritable;

import mapreduce.hi.HIKey;
import mapreduce.hi.api.ChainConfigurator;
import mapreduce.hi.api.Configurator;
import mapreduce.hi.api.input.CustomInputFormat;
import mapreduce.hi.api.interval.custom.CustomCombiner;
import mapreduce.hi.api.interval.custom.CustomMapper;
import mapreduce.hi.api.interval.custom.CustomReducer;
import mapreduce.hi.api.output.CustomOutputFormat;
import mapreduce.hi.api.output.db.CustomDBoutputFormat;
import mapreduce.hi.api.output.db.HIKeyDBWritable;
import mapreduce.hi.api.output.db.HInfoDBWritable;

public class DBConfigurator implements Configurator {

	@Override
	public Job getJob(Configuration conf) throws IOException {
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "oracle.jdbc.driver.OracleDriver");
		conf.set(DBConfiguration.URL_PROPERTY, "jdbc:oracle:thin:@192.168.1.111:1521:ORCL");
		conf.set(DBConfiguration.USERNAME_PROPERTY, "bourntec");
		conf.set(DBConfiguration.PASSWORD_PROPERTY, "bourntec");
		conf.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, "test_info");
		conf.setStrings(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, new String[] { "obj_id","oid","date_time","value","interval" });
		//conf.setInt(DBConfiguration.OUTPUT_FIELD_COUNT_PROPERTY, 5);
		
		
		Job job = new Job(conf, "INTERVAL_LEVEL");
		job.setJarByClass(ChainConfigurator.class);
		job.setInputFormatClass(CustomInputFormat.class);
		job.setMapperClass(DBMapper.class);
		job.setCombinerClass(DBCombiner.class);
		job.setReducerClass(DBReducer.class);
		job.setOutputKeyClass(HIKeyDBWritable.class);
		job.setOutputValueClass(HInfoDBWritable.class);
		job.setOutputFormatClass(CustomDBoutputFormat.class);
		
		return job;
	}
	@Override
	public Job getJob(Configuration conf, String[] otherArgs) throws IOException {
		if (otherArgs.length < 2) {
			System.err
					.println("Usage: Comment <in1 path> <out table> <columns>");
			System.exit(2);
		}
		
		Job job = getJob(conf);
		
		CustomInputFormat.setInputPaths(job, otherArgs[0]);
		
		if(otherArgs.length > 2){//if table + fields[] have given
			String []fields = new String[otherArgs.length-2];
			for(int i=2;i<otherArgs.length;i++){
				fields[i-2] = otherArgs[i];
			}
			CustomDBoutputFormat.setOutput(job, otherArgs[1], fields);
		}else{
			CustomDBoutputFormat.setOutput(job,otherArgs[1]);
		}
		return job;
	}


}
