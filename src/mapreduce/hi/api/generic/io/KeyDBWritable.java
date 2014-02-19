package mapreduce.hi.api.generic.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public abstract class KeyDBWritable<K> implements DBWritable,WritableComparable<K>{	
	
}

	