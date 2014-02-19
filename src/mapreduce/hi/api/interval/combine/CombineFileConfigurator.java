package mapreduce.hi.api.interval.combine;

import java.io.IOException;

import mapreduce.hi.HIKey;
import mapreduce.hi.api.ChainConfigurator;
import mapreduce.hi.api.Configurator;
import mapreduce.hi.api.input.combine.CombineInputFormat;
import mapreduce.hi.api.interval.custom.CustomCombiner;
import mapreduce.hi.api.interval.custom.CustomMapper;
import mapreduce.hi.api.interval.custom.CustomReducer;
import mapreduce.hi.api.output.CustomOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.radiant.cisms.hdfs.seq.HInfoWritable;

public class CombineFileConfigurator implements Configurator {

	@Override
	public Job getJob(Configuration conf) throws IOException {
		Job job = new Job(conf, "INTERVAL_LEVEL");
		job.setJarByClass(ChainConfigurator.class);
		job.setInputFormatClass(CombineInputFormat.class);
		job.setMapperClass(CustomMapper.class);
		job.setCombinerClass(CustomCombiner.class);
		job.setReducerClass(CustomReducer.class);
		job.setOutputKeyClass(HIKey.class);
		job.setOutputValueClass(HInfoWritable.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		return job;
	}

	@Override
	public Job getJob(Configuration conf, String[] otherArgs) throws IOException {
		
		if (otherArgs.length != 2) {
			System.err
					.println("Usage: Comment <in1 path> <out1 path>");
			System.exit(2);
		}
		
		ChainConfigurator.delete(otherArgs[1], conf);
		
		Job job = getJob(conf);
		// CombineInputFormat.addInputPath(job, new Path(otherArgs[0]));
		CombineInputFormat.setInputPaths(job, otherArgs[0]);
		LazyOutputFormat.setOutputFormatClass(job, CustomOutputFormat.class);
		CustomOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//FileOutputFormat.setOutputPath(valueJob, new Path(otherArgs[3]));
		return job;
	}

}
