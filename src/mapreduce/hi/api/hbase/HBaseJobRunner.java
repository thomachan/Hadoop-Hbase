package mapreduce.hi.api.hbase;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import mapreduce.hi.api.Configurator;
import mapreduce.hi.api.ConfiguratorFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;

public class HBaseJobRunner {

	public static void main(String[] args) {
		
		UserGroupInformation ugi = UserGroupInformation
				.createRemoteUser("root");
		

		try {

			ugi.doAs(new PrivilegedExceptionAction<Void>() {

					public Void run() throws Exception {
							Configuration conf = HBaseConfiguration.create();
							conf.set("hbase.zookeeper.quorum", "hadoop"); 
							conf.set("hbase.nameserver.address", "hadoop");
							conf.set("hadoop.job.ugi", "root");
							conf.set("fs.default.name", "hdfs://hadoop:9000");
							conf.set("mapred.job.tracker", "hadoop:9001");
							/*conf.set("hbase.zookeeper.property.clientPort", "2181");
							conf.set("mapred.job.tracker","192.168.1.149:9001");
							conf.set("mapred.child.java.opts", "-Xmx256m");
							conf.set("mapred.job.tracker", "local");
*/
							Configurator configurator = ConfiguratorFactory.get("hbase");
							Job valueJob;
							try {
								valueJob = configurator.getJob(conf);
								valueJob.waitForCompletion(true);
								
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (ClassNotFoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							return null;
					}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
