package hbase.client;

import hdfs.HDFSClient;

import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import javax.naming.NamingException;

import mapreduce.hi.api.generic.input.GenericReader;
import mapreduce.hi.api.hbase.HBASEConstants;
import mapreduce.hi.api.input.defaultcustom.Value;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.UserGroupInformation;

public class HBaseClient {
	static Configuration conf;
	static HTablePool pool;
	private static Logger logger;
	static HTableInterface usersTable;
	 private static SimpleDateFormat sfrmt =  new SimpleDateFormat("dd-MMM-yyyy hh:mma");
	 public static void main(String[] args) throws IOException, ParseException
	 {
		 int size =50;
		init(); 
		initLogger();
		createPool(HBASEConstants.TABLE_HOUR_INFO,size); 
		//createTable(HBASEConstants.TABLE_HINFO,new String[]{"time","info"});
		//createTable(HBASEConstants.TABLE_HOUR_INFO,new String[]{"time","info"});
		//deleteTable(HBASEConstants.TABLE_HINFO);
		//readFromHDFS("infodata/1389597754000");
		//put(HBASEConstants.TABLE_HINFO);
		read(HBASEConstants.TABLE_HOUR_INFO);
		//multipleReaderTest(HBASEConstants.TABLE_HOUR_INFO,size-10);
		closePool(HBASEConstants.TABLE_HOUR_INFO);
		closePool();
		
		//test(HBASEConstants.TABLE_HINFO);
	 }

	private static void deleteTable(String table) throws IOException {
		 HBaseAdmin hba = new HBaseAdmin( conf );
		 hba.disableTable(table);
		 hba.deleteTable(table);
	}
	private static void test(String tableName) throws IOException{
		  HTable table = new HTable(conf, tableName);
	        Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
	        
	        HServerAddress regionServerAddress = table.getRegionLocation(keys.getFirst()[0]).getServerAddress();
	        InetAddress regionAddress =   regionServerAddress.getInetSocketAddress().getAddress();
	       String regionLocation = regionServerAddress.getHostname();
	       try {
	    	   
	    	         String nameServer = conf.get("hbase.nameserver.address", "hadoop");
					regionLocation = DNS.reverseDns(regionAddress, nameServer );
	    	   } catch (NamingException e) {
	    		   
	    	   }
	       System.out.println(regionLocation);
	}

	private static void read(String tableName) throws IOException, ParseException{
		
		new HBaseClient().readWithlogger(tableName,logger);
	}

	public void readWithlogger(String tableName, Logger logger) throws ParseException,
			IOException {
		logger.info("------------------------------------------------------\n\t\t Table : "+tableName+"\n------------------------------------------------------ \n");
   		 logger.info("---------------------- Contents ---------------------");
   		 long start = new Date().getTime();
   		 // filter for object id = 101
		Filter filter = new SingleColumnValueFilter(HBASEConstants.CF_INFO, HBASEConstants.OBJ_ID, CompareOp.EQUAL, Bytes.toBytes(133l)); 
		 Scan scan = new Scan();
		 scan.setFilter(filter);
		 //Filter fRowFilter = new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(1391499233000l)));
		 //scan.setFilter(fRowFilter);
		 
//		 Filter pFilter = new PrefixFilter(Bytes.toBytes("139149923"));
//		 scan.setFilter(pFilter);
		 
		 
		 String startTime= Long.toString(sfrmt.parse("05-Feb-2014 08:30AM").getTime());
		 String endTime = Long.toString(sfrmt.parse("05-Feb-2014 01:25PM").getTime());
		 
		 scan.setStartRow(Bytes.toBytes(startTime)).setStopRow(Bytes.toBytes(endTime));
		 
	     //  scan.setStartRow(Bytes.add(Bytes.toBytes(startTime),Bytes.toBytes("$"))).setStopRow(Bytes.add(Bytes.toBytes(endTime),Bytes.toBytes("$"),Bytes.toBytes("109")));
	        HTableInterface usersTable = pool.getTable(tableName);
	        ResultScanner scanner = usersTable.getScanner(scan);
	        int count= 0;
	        StringBuffer buffer;
	        buffer= new StringBuffer();
        	buffer.append("OBJ_ID");
        	buffer.append("\t");
        	
        	buffer.append("TIME(D-M-Y h:m a)");
        	buffer.append("\t");
        	buffer.append("INFO_ID");
        	buffer.append("\t");
        	buffer.append("COUNT");
        	buffer.append("\t");
        	buffer.append("AVG_VALUE");
        	
        	logger.info(buffer.toString());
        	logger.info("------------------------------------------------------\n");
	        for (Result rs = scanner.next(); rs != null; rs = scanner.next(),count++) {
	         // System.out.println(Bytes.toString(rs.getRow()));
	        	buffer= new StringBuffer();
	        	buffer.append(Bytes.toLong(rs.getValue(HBASEConstants.CF_INFO, HBASEConstants.OBJ_ID)));
	        	buffer.append("\t");
	        	
	        	buffer.append(sfrmt.format(new Date(Bytes.toLong(rs.getValue(HBASEConstants.CF_TIME, HBASEConstants.TIME_STAMP)))));
	        	buffer.append("\t");
	        	buffer.append(Bytes.toString(rs.getValue(HBASEConstants.CF_INFO,HBASEConstants.OID)));
	        	buffer.append("\t");
	        	buffer.append(Bytes.toInt(rs.getValue(HBASEConstants.CF_INFO,HBASEConstants.COUNT)));
	        	buffer.append("\t");
	        	buffer.append(Bytes.toDouble(rs.getValue(HBASEConstants.CF_INFO, HBASEConstants.VALUE)));
	        	
	        	logger.info(buffer.toString());
	        }
	      
	       // System.out.println("------------ Count : "+count+" ------------");
	        scanner.close();
	        long end = new Date().getTime();
      		 logger.info("----------------------- End ----------------------");
      		 logger.info("Records read : "+count);
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
	public void readWitoutLogger(String tableName) throws ParseException,
				IOException {
				 // filter for object id = 101
			Filter filter = new SingleColumnValueFilter(HBASEConstants.CF_INFO, HBASEConstants.OBJ_ID, CompareOp.EQUAL, Bytes.toBytes(133l)); 
			 Scan scan = new Scan();
			 scan.setFilter(filter);
			 //Filter fRowFilter = new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(1391499233000l)));
			 //scan.setFilter(fRowFilter);
			 
			// Filter pFilter = new PrefixFilter(Bytes.toBytes("139149923"));
			// scan.setFilter(pFilter);
			 
			 
			 String startTime= Long.toString(sfrmt.parse("04-Feb-2014 06:30PM").getTime());
			 String endTime = Long.toString(sfrmt.parse("05-Feb-2014 10:07AM").getTime());
			 
			 scan.setStartRow(Bytes.toBytes(startTime)).setStopRow(Bytes.toBytes(endTime));
			 
			 //  scan.setStartRow(Bytes.add(Bytes.toBytes(startTime),Bytes.toBytes("$"))).setStopRow(Bytes.add(Bytes.toBytes(endTime),Bytes.toBytes("$"),Bytes.toBytes("109")));
			    HTableInterface usersTable = pool.getTable(tableName);
			    ResultScanner scanner = usersTable.getScanner(scan);
			    int count= 0;
			    StringBuffer buffer;
			    for (Result rs = scanner.next(); rs != null; rs = scanner.next(),count++) {
			     // System.out.println(Bytes.toString(rs.getRow()));
			    	buffer= new StringBuffer();
			    	buffer.append(Bytes.toLong(rs.getValue(HBASEConstants.CF_INFO, HBASEConstants.OBJ_ID)));
			    	buffer.append("\t");
			    	
			    	buffer.append(sfrmt.format(new Date(Bytes.toLong(rs.getValue(HBASEConstants.CF_TIME, HBASEConstants.TIME_STAMP)))));
			    	buffer.append("\t");
			    	buffer.append(Bytes.toInt(rs.getValue(HBASEConstants.CF_INFO,HBASEConstants.COUNT)));
			    	buffer.append("\t");
			    	buffer.append(Bytes.toDouble(rs.getValue(HBASEConstants.CF_INFO, HBASEConstants.VALUE)));
			    	
			    }
			  
			   System.out.println("------------ Count : "+count+" ------------");
			    scanner.close();
	}
	private static void closePool(String table) throws IOException {
		pool.closeTablePool(table);
	}

	private static void closePool() throws IOException {
		pool.close();
	}

	private static List<Value> readFromHDFS(final String basePath) {
		 UserGroupInformation ugi
         = UserGroupInformation.createRemoteUser("root");
		 initLogger();
			final List<Value> list = new ArrayList<Value>();
		 try {
		
		
			ugi.doAs(new PrivilegedExceptionAction<Void>() {

                public Void run() throws Exception {

                	Configuration conf = new Configuration();
            		conf.set("fs.default.name","hdfs://192.168.1.149:9000");
            		conf.set("hadoop.job.ugi", "root");
            			
            		

           		 FileSystem fs = FileSystem.get(conf);
                   		 logger.info("\n\n**** File : "+basePath+" *****");
                   		 logger.info("--------- Contents ---------");
                   		 long start = new Date().getTime();
                   		 FSDataInputStream din = fs.open(new Path(basePath));
                   		 GenericReader<Value> reader = new GenericReader<Value>(din,new byte[]{'$','$','$'});
                   		 Value info = new Value(64 * 1024);
                   		 int k=0;
                   		 int ex =0;
                   		 int rec =0;
                   		 
                   		 do{        			 
                   			 try{
                   				 k = reader.readObject(info, 64 * 1024, 64 * 1024);
                   				 info.read();
                   				//info.setTime(info.getTime()+rec);
                   				put("hinfo",info);
                   				 rec++;
                   			 }catch(Exception e){
                   				 e.printStackTrace();
                   				 ex++;
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
                             	 
                    return null;
                }
            });
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;

	}
	private static void initLogger() {
		logger = getLogger(HDFSClient.class.getName(),"D:/console.log");  
	}
	private static void put(String tableName, Object data) throws IOException {
		Put p = null;
		if(data instanceof Value){
			Value temp = (Value) data;
			p = new Put(Bytes.toBytes(temp.getTime()+"$"+temp.getObjId()+"$"+temp.getOid()));
			p.add(HBASEConstants.CF_TIME,HBASEConstants.TIME_STAMP,Bytes.toBytes(temp.getTime()));
			p.add(HBASEConstants.CF_INFO,HBASEConstants.OBJ_ID,Bytes.toBytes(temp.getObjId()));
			p.add(HBASEConstants.CF_INFO,HBASEConstants.VALUE,Bytes.toBytes(temp.getValue()));
			p.add(HBASEConstants.CF_INFO,HBASEConstants.OID,Bytes.toBytes(temp.getOid()));
			p.add(HBASEConstants.CF_INFO,HBASEConstants.COUNT, Bytes.toBytes(0));
		}
		
		
		
		usersTable.put(p);
		usersTable.flushCommits();
	}
	private static void put(String tableName) throws IOException {
		Put p = null;
			p = new Put(Bytes.toBytes("1391499233000$109$22"));
			p.add(HBASEConstants.CF_TIME,HBASEConstants.TIME_STAMP,Bytes.toBytes("1391499233000"));
			p.add(HBASEConstants.CF_INFO,HBASEConstants.OBJ_ID,Bytes.toBytes("109"));
			p.add(HBASEConstants.CF_INFO,HBASEConstants.VALUE,Bytes.toBytes("25"));
			p.add(HBASEConstants.CF_INFO,HBASEConstants.OID,Bytes.toBytes("22"));
			p.add(HBASEConstants.CF_INFO,HBASEConstants.COUNT, Bytes.toBytes(0));
		
		usersTable.put(p);
		usersTable.flushCommits();
	}

	private static void createPool(String tablename,int size) throws IOException {
		pool = new HTablePool(conf,size);
		usersTable = pool.getTable(tablename);
	}

	private static void init() {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop"); 
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.setInt("hbase.client.scanner.caching", 20);
	}

	private static void createTable(String table, String[] cfly) throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		
		
		
	  HTableDescriptor ht = new HTableDescriptor(table); 
	  for(String c: cfly){
		  HColumnDescriptor cf = new HColumnDescriptor(c);
		  ht.addFamily(cf);
	  }


	  System.out.println( "connecting" );

	  HBaseAdmin hba = new HBaseAdmin( conf );

	  System.out.println( "Creating Table" );

	  hba.createTable( ht );

	  System.out.println("Done......");
	  hba.close();
	}
	
	private static void multipleReaderTest(final String tableName, final int nStreams) {
		 for (int i=0;i<nStreams;i++){
			 final int itr =i;
			 new Thread(new Runnable(){
				 
				@Override
				public void run() {
					Logger logger = getLogger(tableName+"-"+itr, "D:/multipleInStreamTest/console-"+itr);
					try {
						//new HBaseClient().readWithlogger(tableName, logger);
						new HBaseClient().readWitoutLogger(tableName);
					} catch (ParseException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				 
			}).start();
			/* try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
       		 
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
