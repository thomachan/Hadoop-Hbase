package hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import mapreduce.hi.api.generic.input.GenericReader;
import mapreduce.hi.api.input.defaultcustom.Value;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.security.UserGroupInformation;

import com.radiant.cisms.hdfs.seq.HInfoWritable;

public class HDFSClient {
	static Configuration conf;
	private static Logger logger;
	private static List<FSDataInputStream> pool;
	static FileSystem fs;
	private static void initLogger() {
		logger = Logger.getLogger(HDFSClient.class.getName());  
	    FileHandler fh;  

	    try {  

	        // This block configure the logger with handler and formatter  
	        fh = new FileHandler("D:/console.log");  
	        logger.addHandler(fh);
	        logger.setUseParentHandlers(false);
	       
	        fh.setFormatter(new Formatter() {
	           
	          //  private final DateFormat df = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS");
	         
	            public String format(LogRecord record) {
	                StringBuilder builder = new StringBuilder(1000);
	               /* builder.append("[").append(record.getSourceClassName()).append(".");
	                builder.append(record.getSourceMethodName()).append("] - ");
	                builder.append("[").append(record.getLevel()).append("] - ");*/
	                builder.append(formatMessage(record));
	                builder.append("\n");
	                return builder.toString();
	            }
	         
	            public String getHead(Handler h) {
	                return super.getHead(h);
	            }
	         
	            public String getTail(Handler h) {
	                return super.getTail(h);
	            }
	        });  

	    } catch (SecurityException e) {  
	        e.printStackTrace();  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    }  
		
	}
	public void addFile(String source, String dest) throws IOException {

		FileSystem fileSystem = FileSystem.get(conf);

		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source
				.length());

		// Create the destination path including the filename.
		if (dest.charAt(dest.length() - 1) != '/') {
			dest = dest + "/" + filename;
		} else {
			dest = dest + filename;
		}

		// Check if the file already exists
		Path path = new Path(dest);
		if (fileSystem.exists(path)) {
			System.out.println("File " + dest + " already exists");
			return;
		}

		// Create a new file and write data to it.
		FSDataOutputStream out = fileSystem.create(path);
		InputStream in = new BufferedInputStream(new FileInputStream(new File(
				source)));

		byte[] b = new byte[1024];
		int numBytes = 0;
		while ((numBytes = in.read(b)) > 0) {
			out.write(b, 0, numBytes);
		}

		// Close all the file descripters
		in.close();
		out.close();
		fileSystem.close();
	}
	public void addFromFile(String source, String dest, int limt) throws IOException {

		FileSystem fileSystem = FileSystem.get(conf);

		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source
				.length());

		// Create the destination path including the filename.
		if (dest.charAt(dest.length() - 1) != '/') {
			dest = dest + "/" + filename;
		} else {
			dest = dest + filename;
		}

		// Check if the file already exists
		Path path = new Path(dest);
		if (fileSystem.exists(path)) {
			System.out.println("File " + dest + " already exists");
			return;
		}

		// Create a new file and write data to it.
		FSDataOutputStream out = fileSystem.create(path);
		BufferedReader br = new BufferedReader(new FileReader(new File(source)));

		String str = null;
		int i=0;
		while ((str = br.readLine()) != null && i<limt) {
			HInfoWritable w = new HInfoWritable();
			StringTokenizer itr = new StringTokenizer(str,"||");
			
			if (itr.hasMoreTokens()) {
				//read a single line stroed in hdfs
				w.setObjId(Long.valueOf(itr.nextToken()));
				w.setOid(itr.nextToken());
				w.setValue(Double.valueOf(itr.nextToken()));
				w.setTime(Long.valueOf(itr.nextToken()));
			}
			w.write(out);
			out.write(new byte[]{'$','$','$'});
			i++;
		}

		// Close all the file descripters
		out.close();
		fileSystem.close();
	}
	public void addObject(Object obj, String dest, String filename) throws IOException {

		FileSystem fileSystem = FileSystem.get(conf);


		// Create the destination path including the filename.
		if (dest.charAt(dest.length() - 1) != '/') {
			dest = dest + "/" + filename;
		} else {
			dest = dest + filename;
		}

		// Check if the file already exists
		Path path = new Path(dest);
		if (fileSystem.exists(path)) {
			System.out.println("File " + dest + " already exists");
			return;
		}
		
		// Create a new file and write data to it.
		FSDataOutputStream out = fileSystem.create(path);
		InputStream in = new BufferedInputStream(new ByteArrayInputStream(SerializeHelper.serialize(obj)));

		byte[] b = new byte[1024];
		int numBytes = 0;
		while ((numBytes = in.read(b)) > 0) {
			out.write(b, 0, numBytes);
		}

		// Close all the file descripters
		in.close();
		out.close();
		fileSystem.close();
	}

	public void getHostnames() throws IOException {
		FileSystem fs = FileSystem.get(conf);
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

		String[] names = new String[dataNodeStats.length];
		for (int i = 0; i < dataNodeStats.length; i++) {
			names[i] = dataNodeStats[i].getHostName();
			System.out.println((dataNodeStats[i].getHostName()));
		}
	}

	public void mkdir(String dir) throws IOException {

		FileSystem fileSystem = FileSystem.get(conf);

		Path path = new Path(dir);
		if (fileSystem.exists(path)) {
			System.out.println("Dir " + dir + " already exists!");
			return;
		}

		fileSystem.mkdirs(path);

		fileSystem.close();
	}
	public void copyMerge(String sourceDir, String destFile) throws IOException{
		FileSystem fileSystem = FileSystem.get(conf);
		FileUtil.copyMerge(fileSystem, new Path(sourceDir), fileSystem, new Path(destFile),true, conf, null);

	}
	public void delete(String path) throws IOException{
		FileSystem fileSystem = FileSystem.get(conf);
		fileSystem.delete(new Path(path), true);
	}
	public void readFile(String file) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);

		Path path = new Path(file);
		if (!ifExists(path)) {
			System.out.println("File " + file + " does not exists");
			return;
		}

		FSDataInputStream in = fileSystem.open(path);

		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		while((line = br.readLine())!= null){
			System.out.println(line);
		}
		in.close();
		br.close();
		fileSystem.close();
	}
	public void readObject(String file) throws IOException, ClassNotFoundException {
		FileSystem fileSystem = FileSystem.get(conf);

		Path path = new Path(file);
		if (!fileSystem.exists(path)) {
			System.out.println("File " + file + " does not exists");
			return;
		}

		FSDataInputStream in = fileSystem.open(path);
		Object obj = SerializeHelper.deserialize(in) ;
		System.out.println(obj);
		in.close();
		fileSystem.close();
	}

	public boolean ifExists(Path source) throws IOException {

		FileSystem hdfs = FileSystem.get(conf);
		boolean isExists = hdfs.exists(source);
		System.out.println(isExists);
		return isExists;
	}
	public void append(Path source,String bytes) throws IOException {

		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(source)){
			FSDataOutputStream fsout = hdfs.append(source);
			fsout.writeChars(bytes);
			fsout.close();
			System.out.println("written...");
		}
	}
	public void listContent(String path){
		 try{
             FileSystem fs = FileSystem.get(conf);
             FileStatus[] status = fs.listStatus(new Path(path));  // you need to pass in your hdfs path
             for (int i=0;i<status.length;i++){
            	 if(!status[i].isDir()){
            		 System.out.println("\n\n**** File : "+status[i].getPath()+" *****");
            		 System.out.println("--------- Contents ---------");
                     BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                     String line;
                     line=br.readLine();
                     while (line != null){
                             System.out.println(line);
                             line=br.readLine();
                     }
                     System.out.println("--------- End ---------");
            	 }else{
            		 System.out.println("\n\n**** Direcory : "+status[i].getPath()+" *****");
            	 }
             }
     }catch(Exception e){
             System.out.println("File not found");
     }
	}
	public static void main(String a[]) {
		 UserGroupInformation ugi
         = UserGroupInformation.createRemoteUser("root");

		 try {
		
		
			ugi.doAs(new PrivilegedExceptionAction<Void>() {

                public Void run() throws Exception {

                	conf = new Configuration();
            		conf.set("fs.default.name","hdfs://192.168.1.149:9000");
            		conf.set("hadoop.job.ugi", "root");
            	//	conf.set("dfs.support.append", "true");
//            		conf.addResource(new Path("/usr/local/hadoop-1.0.3/conf/core-site.xml"));
//            		conf.addResource(new Path("/usr/local/hadoop-1.0.3/conf/hdfs-site.xml"));
//            		conf.addResource(new Path("/usr/local/hadoop-1.0.3/conf/mapred-site.xml"));
            		init();
                    return null;
                }
            });
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void write(HInfoWritable info, String path) throws IOException {
		FileSystem fs = FileSystem.get(conf);

		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
				new Path(path), Text.class, HInfoWritable.class,
				SequenceFile.CompressionType.NONE, new DefaultCodec());
		try {
			
			for(int i=0;i<10;i++){
				Text key = new Text();
				key.set(info.toString()+i);
				writer.append(key, info);
			}
		} finally {
			writer.close();
		}
	}
	 public  void read(String string) throws IOException {
		    FileSystem fs = FileSystem.get(conf);

		    SequenceFile.Reader reader =  new SequenceFile.Reader(fs, new Path(string), conf);

		    try {
		      System.out.println(
		          "Is block compressed = " + reader.isBlockCompressed());

		      Text key = new Text();
		      HInfoWritable value = new HInfoWritable();

		      while (reader.next(key, value)) { 
		        System.out.println(key + "," + value);
		      }
		    } finally {
		      reader.close();
		    }
		  }
	 public void hInfoReader(String basepath, String fileStartsWith) throws IOException{
		 FileSystem fs = FileSystem.get(conf);
         FileStatus[] status = fs.listStatus(new Path(basepath));  // you need to pass in your hdfs path
         for (int i=0;i<status.length;i++){
        	 if(!status[i].isDir() && status[i].getPath().getName().startsWith(fileStartsWith)){
        		 logger.info("\n\n**** File : "+status[i].getPath().getName()+" *****");
        		 logger.info("--------- Contents ---------");
        		 long start = new Date().getTime();
        		 FSDataInputStream din = fs.open(status[i].getPath());
        		 GenericReader<Value> reader = new GenericReader<Value>(din,new byte[]{'$','$','$'});
        		 Value info = new Value(64 * 1024);
        		 int k=0;
        		 int ex =0;
        		 int rec =0;
        		 List<Value> data = new ArrayList<Value>();
        		 
        		 do{        			 
        			 try{
        				 k = reader.readObject(info, 64 * 1024, 64 * 1024);
        				 info.read();
        				// data.add(info);
        				 rec++;
        			 }catch(Exception e){
        				 e.printStackTrace();
        				 ex++;
        				// break;
        			 }
        			logger.info(info.toString());
        		 }while(k>0);
        		 din.close();
        		 long end = new Date().getTime();
        		 logger.info("--------- End ---------");
        		 logger.info("Exceptions : "+ex);
        		 logger.info("Records read : "+rec);
        		 long timeRemains= end - start;
				logger.info("Time taken : "+String.format("%d days, %d hrs, %d mins, %d s, %d ms", 
     				    TimeUnit.MILLISECONDS.toDays(timeRemains),
    				    TimeUnit.MILLISECONDS.toHours(timeRemains)-
    				    TimeUnit.DAYS.toHours(TimeUnit.MILLISECONDS.toDays(timeRemains)),
    				    TimeUnit.MILLISECONDS.toMinutes(timeRemains)-
    				    TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(timeRemains)),
    				    TimeUnit.MILLISECONDS.toSeconds(timeRemains) - 
    				    TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(timeRemains)),
    				    timeRemains - 
    				    TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(timeRemains))));
        	 }
         }
	 }
	 public void hInfoReaderSingle(String path) throws IOException{
		 FileSystem fs = FileSystem.get(conf);
        		 logger.info("\n\n**** File : "+path+" *****");
        		 logger.info("--------- Contents ---------");
        		 FSDataInputStream din = fs.open(new Path(path));
        		 GenericReader<Value> reader = new GenericReader<Value>(din,new byte[]{'$','$','$'});
        		 Value info = new Value(64 * 1024);
        		 int k=0;
        		 int ex =0;
        		 int rec =0;
        		 do{        			 
        			 try{
        				 k = reader.readObject(info, 64 * 1024, 64 * 1024);
        				 info.read();
        				 rec++;
        			 }catch(Exception e){
        				 e.printStackTrace();
        				 ex++;
        				// break;
        			 }
        		//	logger.info(info.toString());
        		 }while(k>0);
        		 din.close();
        		 logger.info("--------- End ---------");
        		 logger.info("Exceptions : "+ex);
        		 logger.info("Records read : "+rec);
	 }
	protected static void init() throws IOException, ClassNotFoundException {
		initLogger();
		String []args = {"D:/haddop-testdata/3.txt","custom/file01"};
		//String []args = {"hiarchive/output/103"};
		HDFSClient client = new HDFSClient();
			
			//client.append(new Path("append/1/text.txt"), "sample appended");
			//client.addFile(args[0], args[1]);
			//client.readFile("infodata/1389067571000");
			//client.listContent("/test/hi");
			//client.listFiles("infodata");
			//client.hInfoReader("out1","HOUR");
			//client.hInfoReaderSingle("infodata/1389597754000");
			//client.read("infodata/1389067571000");
			//client.write(new HInfoWritable("redhat:1.6",111l,1384433196012l,5), "test/historical/113");
			//System.out.println(client.ifExists(new Path(args[0])));
			client.mkdir("hbase");
			//client.ifExists(new Path("/hbase"));
			//client.getHostnames();
			//client.addObject(new Text("Twitter","msg"), "test", "text02");
			//client.readObject("test/text01");
		//client.copyMerge("historical", "temp");
		//client.delete("out1");
		//client.writeString("sdfkdshf", "sample/input");
		//client.readString("sample/input");
		//client.addFromFile(args[0], args[1],100);
		//client.write(new HInfoWritable("cvbvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv",111l,1384433196012l,5));
		//client.writeFromFile("D:/console1.log","/test/currupted");

		//client.multipleInStreamTest("infodata/1390548530000",3);
		//client.inStreamPoolTest("infodata/1390548530000",10);
		System.out.println("Done!");
		
	}
	private void inStreamPoolTest(final String file, int nStreams) {
		try{
		createStramPool(file,nStreams);
		 for (int i=0;i<nStreams;i++){
			 final int itr =i;
			 new Thread(new Runnable(){
				 
				@Override
				public void run() {
					Logger logger = getLogger(file+"-"+itr, "D:/multipleInStreamTest/console-"+itr);
	        		 logger.info("--------- Contents ---------");
	        		 long start = new Date().getTime();
	        		 
					
						FSDataInputStream din = getStream(itr);
					
	        		 GenericReader<Value> reader = new GenericReader<Value>(din,new byte[]{'$','$','$'});
	        		 Value info = new Value(1024);
	        		 int k=0;
	        		 int ex =0;
	        		 int rec =0;
	        		// List<Value> data = new ArrayList<Value>();
	        		 
	        		/* BufferedReader br=new BufferedReader(new InputStreamReader(din));
	                 String line;
	                 
	                 try {
						while ((line=br.readLine()) != null){
						         rec++;
						         if(rec >= 10000)
						        	 break;
						 }
					} catch (IOException e1) {
						e1.printStackTrace();
					}*/
	                 
	        		 do{        			 
	        			 try{
	        				 k = reader.readObject(info, 1024, 1024);
	        				 info.read();
	        				// data.add(info);
	        				 rec++;
	        			 }catch(Exception e){
	        				 e.printStackTrace();
	        				 ex++;
	        				// break;
	        			 }
	        			//logger.info(info.toString());
	        		 }while(rec>10000);
	        		
					
	        		 long end = new Date().getTime();
	        		 logger.info("--------- End ---------");
	        		 logger.info("Exceptions : "+ex);
	        		 logger.info("Records read : "+rec);
	        		 long timeRemains= end - start;
					logger.info("Time taken : "+String.format("%d days, %d hrs, %d mins, %d s, %d ms", 
	     				    TimeUnit.MILLISECONDS.toDays(timeRemains),
	    				    TimeUnit.MILLISECONDS.toHours(timeRemains)-
	    				    TimeUnit.DAYS.toHours(TimeUnit.MILLISECONDS.toDays(timeRemains)),
	    				    TimeUnit.MILLISECONDS.toMinutes(timeRemains)-
	    				    TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(timeRemains)),
	    				    TimeUnit.MILLISECONDS.toSeconds(timeRemains) - 
	    				    TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(timeRemains)),
	    				    timeRemains - 
	    				    TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(timeRemains))));
					
					try {
						pool.get(itr).close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					
				}

				 
			 	}
			 ).start();
       		 
       	 }
		}catch(Exception e){
			
		}finally{
			System.out.println("finished !");
		}
		
	
	}
	protected static FSDataInputStream getStream(int itr) {
		if(pool != null && pool.size()>= itr+1)
			return  pool.get(itr);
		return null;
	}
	private static void createStramPool(String file, int nStreams) {
		pool =  new ArrayList<FSDataInputStream>();
		try {
			for (int i = 0; i < nStreams; i++) {
				fs = FileSystem.get(conf);
				FSDataInputStream din = fs.open(new Path(file));
				pool.add(din);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	private static void destroyPool(){
		try {
			for (int i = 0; i < pool.size(); i++) {
				pool.get(i).close();;
			}
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private void multipleInStreamTest(final String file, final int nStreams) {
		 for (int i=0;i<nStreams;i++){
			 final int itr =i;
			 new Thread(new Runnable(){
				 
				@Override
				public void run() {
					Logger logger = getLogger(file+"-"+itr, "D:/multipleInStreamTest/console-"+itr);
	        		 logger.info("--------- Contents ---------");
	        		 long start = new Date().getTime();
	        		 FileSystem fs;
	        		 FSDataInputStream din = null;
					try {
						fs = FileSystem.get(conf);
					
						din = fs.open(new Path(file));
					
	        		 GenericReader<Value> reader = new GenericReader<Value>(din,new byte[]{'$','$','$'});
	        		 Value info = new Value(64 * 1024);
	        		 int k=0;
	        		 int ex =0;
	        		 int rec =0;
	        		// List<Value> data = new ArrayList<Value>();
	        		 do{        			 
	        			 try{
	        				 k = reader.readObject(info, 64 * 1024, 64 * 1024);
	        				// info.read();
	        				// data.add(info);
	        				 rec++;
	        			 }catch(Exception e){
	        				 e.printStackTrace();
	        				 ex++;
	        				// break;
	        			 }
	        			logger.info(info.toString());
	        		 }while(k>0);
	        		
						din.close();
					
	        		 long end = new Date().getTime();
	        		 logger.info("--------- End ---------");
	        		 logger.info("Exceptions : "+ex);
	        		 logger.info("Records read : "+rec);
	        		 long timeRemains= end - start;
					logger.info("Time taken : "+String.format("%d days, %d hrs, %d mins, %d s, %d ms", 
	     				    TimeUnit.MILLISECONDS.toDays(timeRemains),
	    				    TimeUnit.MILLISECONDS.toHours(timeRemains)-
	    				    TimeUnit.DAYS.toHours(TimeUnit.MILLISECONDS.toDays(timeRemains)),
	    				    TimeUnit.MILLISECONDS.toMinutes(timeRemains)-
	    				    TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(timeRemains)),
	    				    TimeUnit.MILLISECONDS.toSeconds(timeRemains) - 
	    				    TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(timeRemains)),
	    				    timeRemains - 
	    				    TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(timeRemains))));
					} catch (IOException e) {
						e.printStackTrace();
					}
					
				}
				 
			 	}
			 ).start();
        		 
        	 }
		
	}
	private void write(HInfoWritable w) throws IOException {
		
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path("/test/hi");
		if (fileSystem.exists(path)) {
			System.out.println("File /test/hi  already exists");
			delete("/test/hi");
			return;
		}
		FSDataOutputStream out = fileSystem.create(path);
		for(int i=0;i<10000;i++){
			
			w.write(out);
			out.write(new byte[]{'$','$','$'});
			if(i>9000){
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("Written :"+w);
			}
		}
	}

	private void writeFromFile(String source, String dest) throws IOException {


		FileSystem fileSystem = FileSystem.get(conf);

		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source
				.length());

		// Create the destination path including the filename.
		if (dest.charAt(dest.length() - 1) != '/') {
			dest = dest + "/" + filename;
		} else {
			dest = dest + filename;
		}

		// Check if the file already exists
		Path path = new Path(dest);
		if (fileSystem.exists(path)) {
			System.out.println("File " + dest + " already exists");
			delete(dest);
			return;
		}

		// Create a new file and write data to it.
		FSDataOutputStream out = fileSystem.create(path);
		BufferedReader br = new BufferedReader(new FileReader(source));

		String str = null;
		int i=0;
		while ((str = br.readLine()) != null) {
			
			out.writeUTF(str);
			try {
				if(i>4100)
					Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println(i++);
			
		}

		// Close all the file descripters
		br.close();
		out.close();
		fileSystem.close();
			
	}
	public void writeText(String content, String file) throws IOException {
		Text t= new Text();
		FileSystem fileSystem = FileSystem.get(conf);
		// Check if the file already exists
		Path path = new Path(file);
		FSDataOutputStream out = null;
		
			  if (fileSystem.exists(path)) {
					System.out.println("File " + file + " already exists");
					FileStatus fileS  = fileSystem.getFileStatus(path);
					fileS.write(out);
				}else{
					System.out.println("File " + file + " created ");
					out = fileSystem.create(path);
				}
		   // wrap the outputstream with a writer
		   PrintWriter writer = new PrintWriter(out);
		   writer.print(content);
		   writer.close();
		out.close();
		fileSystem.close();
	}
	public void writeString(String content, String file) throws IOException {
		Text t= new Text(content);
		FileSystem fileSystem = FileSystem.get(conf);
		// Check if the file already exists
		Path path = new Path(file);
		FSDataOutputStream out = fileSystem.create(path);
		t.write(out);
		out.close();
		fileSystem.close();
	}
	public void readString(String file) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		// Check if the file already exists
		Path path = new Path(file);
		byte[] b = new byte[1024];
		FSDataInputStream in = fileSystem.open(path);
		int length = in.read(b);
		System.out.println(new String(b,0,length));
		in.close();
		fileSystem.close();
	}
	private void listFiles(String path){

		 try{
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status = fs.listStatus(new Path(path));  // you need to pass in your hdfs path
            int i;
            StringBuffer fileList = new StringBuffer();
            for (i=0;i<status.length;i++){
           	 if(!status[i].isDir()){
           		 System.out.println("\n\n**** File : "+status[i].getPath()+" *****");
           		fileList.append(status[i].getPath().toString().replace("hdfs://192.168.1.149:9000/user/root/","")+",");
           	 }else{
           		 System.out.println("\n\n**** Direcory : "+status[i].getPath()+" *****");
           		
           	 }
            }
            System.out.println("\n\nTotal :  "+i); 
            System.out.println("\nComma separated :  "+fileList); 
    }catch(Exception e){
            System.out.println("File not found");
    }
	
	}
	public static Logger getLogger(String name,String file){

		Logger logger = Logger.getLogger(name);  
	    FileHandler fh;  

	    try {  

	        // This block configure the logger with handler and formatter  
	        fh = new FileHandler(file);  
	        logger.addHandler(fh);
	        logger.setUseParentHandlers(false);
	       
	        fh.setFormatter(new Formatter() {
	           
	         
	            public String format(LogRecord record) {
	                StringBuilder builder = new StringBuilder(1000);
	                builder.append(formatMessage(record));
	                builder.append("\n");
	                return builder.toString();
	            }
	         
	            public String getHead(Handler h) {
	                return super.getHead(h);
	            }
	         
	            public String getTail(Handler h) {
	                return super.getTail(h);
	            }
	        });  

	    } catch (SecurityException e) {  
	        e.printStackTrace();  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    }
		return logger;  
		
	
	}
}
