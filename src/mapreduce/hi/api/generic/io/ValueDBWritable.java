package mapreduce.hi.api.generic.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public abstract class ValueDBWritable<V> implements DBWritable,WritableComparable<V>{

}
