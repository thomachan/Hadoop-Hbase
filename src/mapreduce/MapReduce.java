package mapreduce;

import mapreduce.hi.api.Notused;

@Notused(reason="only basic")
public abstract class MapReduce {/*

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {// Parse the input string into a nice map
			String txt = value.toString();
			// .get will return null if the key is not there
			if (txt == null) {
				// skip this record
				return;
			}
			// Tokenize the string by splitting it up on whitespace into
			// something we can iterate over,
			// then send the tokens away
			StringTokenizer itr = new StringTokenizer(txt);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		    	  private IntWritable result = new IntWritable();
		    	  
		    	  public void reduce(Text key, Iterable<IntWritable> values,  Context context) throws IOException, InterruptedException {
			    	  int sum = 0;
			    	  for (IntWritable val : values) {
			    	  sum += val.get();
		    	  }
		    	  result.set(sum);
		    	  context.write(key, result);
		    	  }
		      }

	public static void main(String[] args) throws Exception {
		final String[] a = args;
		UserGroupInformation ugi = UserGroupInformation
				.createRemoteUser("root");

		try {

			ugi.doAs(new PrivilegedExceptionAction<Void>() {

				public Void run() throws Exception {

					Configuration conf = new Configuration();
					conf.set("mapred.job.tracker", "192.168.1.149:9001");
					conf.set("fs.default.name", "hdfs://192.168.1.149:9000");
					conf.set("hadoop.job.ugi", "root");

					String[] otherArgs = new GenericOptionsParser(conf, a)
							.getRemainingArgs();
					if (otherArgs.length != 2) {
						System.err
								.println("Usage: CommentWordCount <in> <out>");
						System.exit(2);
					}
					
					 * conf.setOutputKeyClass(Text.class);
					 * conf.setOutputValueClass(IntWritable.class);
					 * 
					 * 
					 * conf.setInputFormat(TextInputFormat.class);
					 * conf.setOutputFormat(TextOutputFormat.class);
					 * 
					 * 
					 * JobClient.runJob(conf);
					 

					Job job = new Job(conf, "Word Count");
					job.setJarByClass(MapReduce.class);
					job.setMapperClass(Map.class);
					job.setCombinerClass(Reduce.class);
					job.setReducerClass(Reduce.class);
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(IntWritable.class);
					
					 * FileInputFormat.addInputPath(job, new
					 * Path("wordcount/w01"));
					 * FileOutputFormat.setOutputPath(job, new
					 * Path("wordcount/result03"));
					 
					FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
					FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
					System.exit(job.waitForCompletion(true) ? 0 : 1);

					return null;
				}
			});

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
*/}
