package mapreduce.hi.api.interval;

import java.io.IOException;

import mapreduce.hi.HIKey;
import mapreduce.hi.api.ChainConfigurator;
import mapreduce.hi.api.Configurator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.radiant.cisms.hdfs.seq.HInfoWritable;

public class SimpleConfigurator implements Configurator {

	@Override
	public Job getJob(Configuration conf) throws IOException {
		Job job = new Job(conf, "INTERVAL_LEVEL");
		job.setJarByClass(ChainConfigurator.class);
		job.setInputFormatClass(FileInputFormat.class);
		job.setMapperClass(SimpleMapper.class);
		job.setCombinerClass(SimpleReducer.class);
		job.setReducerClass(SimpleReducer.class);
		job.setOutputKeyClass(HIKey.class);
		job.setOutputValueClass(HInfoWritable.class);
		job.setOutputFormatClass(FileOutputFormat.class);
		return job;
	}
	@Override
	public Job getJob(Configuration conf, String[] otherArgs) throws IOException {
		if (otherArgs.length < 2) {
			System.err
					.println("Usage: Comment <in1 path> <out path>");
			System.exit(2);
		}
		
		ChainConfigurator.delete(otherArgs[1], conf);
		Job job = getJob(conf);
		// CombineInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileInputFormat.setInputPaths(job, otherArgs[0]);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
		return job;
	}


}
