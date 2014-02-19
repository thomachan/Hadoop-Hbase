package mapreduce.hi.api.object;

import java.io.IOException;

import mapreduce.hi.api.ChainConfigurator;
import mapreduce.hi.api.Configurator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class ObjectLevelConfigurator implements Configurator{

	@Override
	public Job getJob(Configuration conf) throws IOException{
		Job job = new Job(conf, "OBJECT_LEVEL");
		job.setJarByClass(ChainConfigurator.class);
		job.setMapperClass(ObjectLevelMapper.class);
		//job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		return job;
	}

	@Override
	public Job getJob(Configuration conf, String[] otherArgs)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
}
