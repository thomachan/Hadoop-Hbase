package mapreduce.hi.api.generic.input;

import java.io.IOException;
import java.io.InputStream;

import mapreduce.hi.api.generic.io.BufferedValueWritable;

import org.apache.hadoop.conf.Configuration;

public class GenericReader<V extends BufferedValueWritable<V>> {

	  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
	  private int bufferSize = DEFAULT_BUFFER_SIZE;
	  private InputStream in;
	  private byte[] buffer;
	  // the number of bytes of real data in the buffer
	  private int bufferLength = 0;
	  // the current position in the buffer
	  private int bufferPosn = 0;

	 /* private static final byte CR = '\r';
	  private static final byte LF = '\n';*/

	  // The line delimiter
	  private byte[] recordDelimiterBytes;

	  /**
	   * Create a CustomReader that reads from the given stream using the
	   * default buffer-size (64k).
	   * @param in The input stream
	   * @throws IOException
	   */
	  public GenericReader(InputStream in) {
	    this(in, DEFAULT_BUFFER_SIZE);
	  }

	 

	/**
	   * Create a CustomReader that reads from the given stream using the 
	   * given buffer-size.
	   * @param in The input stream
	   * @param bufferSize Size of the read buffer
	   * @throws IOException
	   */
	  public GenericReader(InputStream in, int bufferSize) {
	    this.in = in;
	    this.bufferSize = bufferSize;
	    this.buffer = new byte[this.bufferSize];
	    this.recordDelimiterBytes = null;
	  }

	  /**
	   * Create a CustomReader that reads from the given stream using the
	   * <code>io.file.buffer.size</code> specified in the given
	   * <code>Configuration</code>.
	   * @param in input stream
	   * @param conf configuration
	   * @throws IOException
	   */
	  public GenericReader(InputStream in, Configuration conf) throws IOException {
	    this(in, DEFAULT_BUFFER_SIZE);
	  }

	  /**
	   * Create a CustomReader that reads from the given stream using the
	   * default buffer-size, and using a custom delimiter of array of
	   * bytes.
	   * @param in The input stream
	   * @param recordDelimiterBytes The delimiter
	   */
	  public GenericReader(InputStream in, byte[] recordDelimiterBytes) {
	    this.in = in;
	    this.bufferSize = DEFAULT_BUFFER_SIZE;
	    this.buffer = new byte[this.bufferSize];
	    this.recordDelimiterBytes = recordDelimiterBytes;
	  }

	  /**
	   * Create a CustomReader that reads from the given stream using the
	   * given buffer-size, and using a custom delimiter of array of
	   * bytes.
	   * @param in The input stream
	   * @param bufferSize Size of the read buffer
	   * @param recordDelimiterBytes The delimiter
	   * @throws IOException
	   */
	  public GenericReader(InputStream in, int bufferSize,
	      byte[] recordDelimiterBytes) {
	    this.in = in;
	    this.bufferSize = bufferSize;
	    this.buffer = new byte[this.bufferSize];
	    this.recordDelimiterBytes = recordDelimiterBytes;
	  }

	  /**
	   * Create a CustomReader that reads from the given stream using the
	   * <code>io.file.buffer.size</code> specified in the given
	   * <code>Configuration</code>, and using a custom delimiter of array of
	   * bytes.
	   * @param in input stream
	   * @param conf configuration
	   * @param recordDelimiterBytes The delimiter
	   * @throws IOException
	   */
	  public GenericReader(InputStream in, Configuration conf,
	      byte[] recordDelimiterBytes) throws IOException {
	    this.in = in;
	    this.bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
	    this.buffer = new byte[this.bufferSize];
	    this.recordDelimiterBytes = recordDelimiterBytes;
	  }


	  /**
	   * Close the underlying stream.
	   * @throws IOException
	   */
	  public void close() throws IOException {
	    in.close();
	  }
	  
	  /**
	   * Read one object from the InputStream of the specified class.
	 * @param maxBytesToConsume 
	 * @param maxRecordLength 
	   *
	   * @throws IOException if the underlying stream throws
	   */
	  public int readObject(V value, long maxBytesToConsume, int maxRecordLength) throws IOException {
	     
	   /* We're reading data from inputStream, but the head of the stream may be
	    *  already captured in the previous buffer, so we have several cases:
	    * 
	    * 1. The buffer tail does not contain any character sequence which
	    *    matches with the head of delimiter. We count it as a 
	    *    ambiguous byte count = 0
	    *    
	    * 2. The buffer tail contains a X number of characters,
	    *    that forms a sequence, which matches with the
	    *    head of delimiter. We count ambiguous byte count = X
	    *    
	    *    // ***  eg: A segment of input file is as follows
	    *    
	    *    " record 1792: I found this bug very interesting and
	    *     I have completely read about it. record 1793: This bug
	    *     can be solved easily record 1794: This ." 
	    *    
	    *    delimiter = "record";
	    *        
	    *    supposing:- String at the end of buffer =
	    *    "I found this bug very interesting and I have completely re"
	    *    There for next buffer = "ad about it. record 179       ...."           
	    *     
	    *     The matching characters in the input
	    *     buffer tail and delimiter head = "re" 
	    *     Therefore, ambiguous byte count = 2 ****   //
	    *     
	    *     2.1 If the following bytes are the remaining characters of
	    *         the delimiter, then we have to capture only up to the starting 
	    *         position of delimiter. That means, we need not include the 
	    *         ambiguous characters in str.
	    *     
	    *     2.2 If the following bytes are not the remaining characters of
	    *         the delimiter ( as mentioned in the example ), 
	    *         then we have to include the ambiguous characters in str. 
	    */
		value.clear();		  

		int byteLength = 0; // tracks buff.getLength(), as an optimization
	    long bytesConsumed = 0;
	    int delPosn = 0;
	    int ambiguousByteCount=0; // To capture the ambiguous characters count
	    do {
	      int startPosn = bufferPosn; // Start from previous end position
	      if (bufferPosn >= bufferLength) {// initailly read byte[] from stream
	        startPosn = bufferPosn = 0;
	        bufferLength = in.read(buffer);
	        if (bufferLength <= 0) {
	        	value.put(recordDelimiterBytes, 0, ambiguousByteCount);
	          break; // EOF
	        }
	      }
	      /*
	       * this loop will position the buffer index
	       * to the starting of one record, by skipping delimiter
	       * 
	       */
	      for (; bufferPosn < bufferLength; ++bufferPosn) {
	        if (buffer[bufferPosn] == recordDelimiterBytes[delPosn]) {
	          delPosn++;
	          if (delPosn >= recordDelimiterBytes.length) {
	            bufferPosn++;
	            break;
	          }
	        } else if (delPosn != 0) {// need to evaluate this case
	          bufferPosn--;
	          delPosn = 0;
	        }
	      }
	      int readLength = bufferPosn - startPosn;// no. of bytes read
	      bytesConsumed += readLength;
	      int appendLength = readLength - delPosn;//effective no. of bytes, ie, delimiter length is avoided
	      if (appendLength > maxRecordLength - byteLength) {
	        appendLength = maxRecordLength - byteLength;
	      }
	      if (appendLength > 0) {
	        if (ambiguousByteCount > 0) {
	         value.put(recordDelimiterBytes, 0, ambiguousByteCount);
	          //appending the ambiguous characters (refer case 2.2)
	          bytesConsumed += ambiguousByteCount;
	          ambiguousByteCount=0;
	        }
	        if(buffer != null && value.getBuff() != null){
		        value.put(buffer, startPosn, appendLength);
		        byteLength += appendLength;
	        }
	      }
	      if (bufferPosn >= bufferLength) {
	        if (delPosn > 0 && delPosn < recordDelimiterBytes.length) {
	          ambiguousByteCount = delPosn;
	          bytesConsumed -= ambiguousByteCount; //to be consumed in next
	        }
	      }
	    } while (delPosn < recordDelimiterBytes.length 
	        && bytesConsumed < maxBytesToConsume);
	    if (bytesConsumed > (long) Integer.MAX_VALUE) {
	      throw new IOException("Too many bytes before delimiter: " + bytesConsumed);
	    }
	    return (int) bytesConsumed; //total bytes consumed(including delimiter length)
	  } 		

}
