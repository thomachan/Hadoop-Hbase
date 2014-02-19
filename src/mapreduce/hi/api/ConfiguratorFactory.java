package mapreduce.hi.api;

import mapreduce.hi.api.hbase.HBaseConfigurator;
import mapreduce.hi.api.interval.combine.CombineFileConfigurator;
import mapreduce.hi.api.interval.custom.CustomConfigurator;
import mapreduce.hi.api.interval.SimpleConfigurator;
import mapreduce.hi.api.interval.db.DBConfigurator;
import mapreduce.hi.api.interval.defaultcustom.DefaultConfigurator;
import mapreduce.hi.api.interval.multifile.MultiFileConfigurator;

public class ConfiguratorFactory {
	
	public static Configurator get(String type) {
		Configurators conf = Configurators.valueOf(type);
		if(conf != null){
			switch(conf){
				case simple:
					return new SimpleConfigurator();
				case custom:
					return new CustomConfigurator();
				case db:
					return new DBConfigurator();
				case multi:
					return new MultiFileConfigurator();
				case combine:
					return new CombineFileConfigurator();
				case defaultcombine:
					return new DefaultConfigurator(true);
				case hbase:
					return new HBaseConfigurator();
				default:
					return new DefaultConfigurator(false);
			}
		}
		
		return null;
	}
	
}
