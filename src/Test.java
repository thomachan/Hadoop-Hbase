import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Test {
public static class Map extends MapReduceBase implements
        Mapper<LongWritable, Text, String, Integer> {

    @Override
    public void map(LongWritable key, Text value,
            OutputCollector<String, Integer> output, Reporter reporter)
            throws IOException {
        String line = value.toString();
        String[] lineParts = line.split(",");
        output.collect(lineParts[0], Integer.parseInt(lineParts[1]));

    }
}

public static class Reduce extends MapReduceBase implements
        Reducer<String, Integer, String, Integer> {

    @Override
    public void reduce(String key, Iterator<Integer> values,
            OutputCollector<String, Integer> output, Reporter reporter)
            throws IOException {
        int sum = 0;
        while (values.hasNext()) {
            sum = sum + values.next();
        }
        output.collect(key, sum);
    }
}

public static void main(String[] args) throws Exception {

    JobConf conf = new JobConf(Test.class);
    conf.setJobName("ProductCount");

    conf.setMapOutputKeyClass(String.class);
    conf.setMapOutputValueClass(Integer.class);

    conf.setOutputKeyClass(String.class);
    conf.setOutputValueClass(Integer.class);

    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);

}
}