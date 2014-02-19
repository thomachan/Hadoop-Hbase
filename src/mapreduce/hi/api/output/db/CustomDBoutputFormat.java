package mapreduce.hi.api.output.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import mapreduce.hi.api.NotFoundException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CustomDBoutputFormat extends OutputFormat<HIKeyDBWritable, HInfoDBWritable> {

  private static final Log LOG = LogFactory.getLog(CustomDBoutputFormat.class);
  public void checkOutputSpecs(JobContext context) 
      throws IOException, InterruptedException {}

  public OutputCommitter getOutputCommitter(TaskAttemptContext context) 
      throws IOException, InterruptedException {
    return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
                                   context);
  }

    /**
   * Constructs the query used as the prepared statement to insert data.
   * 
   * @param table
   *          the table to insert into
   * @param fieldNames
   *          the fields to insert into. If field names are unknown, supply an
   *          array of nulls.
   */
  public String constructQuery(String table, String[] fieldNames) {
    if(fieldNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    }

    StringBuilder query = new StringBuilder();
    query.append("INSERT INTO ").append(table);

    if (fieldNames.length > 0 && fieldNames[0] != null) {
      query.append(" (");
      for (int i = 0; i < fieldNames.length; i++) {
        query.append(fieldNames[i]);
        if (i != fieldNames.length - 1) {
          query.append(",");
        }
      }
      query.append(")");
    }
    query.append(" VALUES (");

    for (int i = 0; i < fieldNames.length; i++) {
      query.append("?");
      if(i != fieldNames.length - 1) {
        query.append(",");
      }
    }
    query.append(")");

    return query.toString();
  }

  @Override
  public RecordWriter<HIKeyDBWritable, HInfoDBWritable> getRecordWriter(TaskAttemptContext context) 
		      throws IOException {
		    DBConfiguration dbConf = new DBConfiguration(context.getConfiguration());
		    String tableName = dbConf.getOutputTableName();
		    String[] fieldNames = dbConf.getOutputFieldNames();
		    
		    if(fieldNames == null) {
		      fieldNames = new String[dbConf.getOutputFieldCount()];
		    }
		  /* String clazz=  context.getConfiguration().get(DBConfiguration.DRIVER_CLASS_PROPERTY);
		    if(!clazz.isEmpty()){
		    	throw new NotFoundException("-----------------------------\n"+
		    clazz+context.getConfiguration().get(DBConfiguration.URL_PROPERTY)+"\n"
		    			+context.getConfiguration().get(DBConfiguration.USERNAME_PROPERTY)+"\n"
		    			+context.getConfiguration().get(DBConfiguration.PASSWORD_PROPERTY)+"\n"
		    			+"----------------------------------");
		    }*/
		   
		      Connection connection = null;
			try {
				connection = dbConf.getConnection();
			} catch (ClassNotFoundException e) {
				throw new NotFoundException("ClassNotFoundException");
			} catch (SQLException e) {
				throw new NotFoundException("SQLException");
			}
		      PreparedStatement statement = null;
		  
		      try {
				statement = connection.prepareStatement(
				                constructQuery(tableName, fieldNames));
			} catch (SQLException e) {
				throw new NotFoundException("SQLException");
			}
		      try {
				return new CustomDBRecordWriter<HIKeyDBWritable,HInfoDBWritable>(connection, statement);
			} catch (SQLException e) {
				throw new NotFoundException("SQLException");
			}
   }

  /**
   * Initializes the reduce-part of the job with 
   * the appropriate output settings
   * 
   * @param job The job
   * @param tableName The table to insert data into
   * @param fieldNames The field names in the table.
   */
  public static void setOutput(Job job, String tableName, 
      String... fieldNames) throws IOException {
    if(fieldNames.length > 0 && fieldNames[0] != null) {
      DBConfiguration dbConf = setOutput(job, tableName);
      dbConf.setOutputFieldNames(fieldNames);
    } else {
      if (fieldNames.length > 0) {
        setOutput(job, tableName, fieldNames.length);
      }
      else { 
        throw new IllegalArgumentException(
          "Field names must be greater than 0");
      }
    }
  }
  
  /**
   * Initializes the reduce-part of the job 
   * with the appropriate output settings
   * 
   * @param job The job
   * @param tableName The table to insert data into
   * @param fieldCount the number of fields in the table.
   */
  public static void setOutput(Job job, String tableName, 
      int fieldCount) throws IOException {
    DBConfiguration dbConf = setOutput(job, tableName);
    dbConf.setOutputFieldCount(fieldCount);
  }
  
  private static DBConfiguration setOutput(Job job,
      String tableName) throws IOException {
    job.setOutputFormatClass(CustomDBoutputFormat.class);
    job.setReduceSpeculativeExecution(false);

    DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
    
    dbConf.setOutputTableName(tableName);
    return dbConf;
  }
}

