package mapreduce.hi.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public interface Configurator {
	public Job getJob(Configuration conf)throws IOException;

	public Job getJob(Configuration conf, String[] otherArgs) throws IOException;
}
