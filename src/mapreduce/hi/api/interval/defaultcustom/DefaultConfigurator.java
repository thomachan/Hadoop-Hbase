package mapreduce.hi.api.interval.defaultcustom;

import java.io.IOException;

import mapreduce.hi.HIKey;
import mapreduce.hi.api.ChainConfigurator;
import mapreduce.hi.api.Configurator;
import mapreduce.hi.api.MRJobConfig;
import mapreduce.hi.api.input.defaultcustom.DefaultCombineInputFormat;
import mapreduce.hi.api.input.defaultcustom.DefaultInputFormat;
import mapreduce.hi.api.input.defaultcustom.Value;
import mapreduce.hi.api.output.defaultcustom.DefaultOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class DefaultConfigurator implements Configurator {

	private boolean isCombined;

	public DefaultConfigurator(boolean isCombined) {
		this.isCombined = isCombined;
	}

	@Override
	public Job getJob(Configuration conf) throws IOException {
		Job job = new Job(conf, "INTERVAL_LEVEL");
		job.setJarByClass(ChainConfigurator.class);
		job.setInputFormatClass(isCombined ? DefaultCombineInputFormat.class : DefaultInputFormat.class);
		job.setMapperClass(DefaultMapper.class);
		job.setCombinerClass(DefaultCombiner.class);
		job.setReducerClass(DefaultReducer.class);
		job.setOutputKeyClass(HIKey.class);
		job.setOutputValueClass(Value.class);
		//job.setOutputFormatClass(DefaultOutputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		return job;
	}

	@Override
	public Job getJob(Configuration conf, String[] otherArgs) throws IOException {
		
		if (otherArgs.length != 3) {
			System.err
					.println("Usage: Comment <in1 path> <out1 path> <type>");
			System.exit(2);
		}
		if(isCombined){
			conf.setLong(MRJobConfig.MR_COMBINE_SPLIT_MAXSIZE, 1024*1024*64l);
		}
		ChainConfigurator.delete(otherArgs[1], conf);
		
		Job job = getJob(conf);
		// CombineInputFormat.addInputPath(job, new Path(otherArgs[0]));
		DefaultCombineInputFormat.setInputPaths(job, otherArgs[0]);
		LazyOutputFormat.setOutputFormatClass(job, DefaultOutputFormat.class);
		DefaultOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		return job;
	}

}
