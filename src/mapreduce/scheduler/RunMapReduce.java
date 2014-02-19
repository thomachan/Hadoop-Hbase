package mapreduce.scheduler;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.util.RunJar;

public class RunMapReduce {
	public static void run(String args[]) throws IOException, InterruptedException{
		ProcessBuilder pb = new ProcessBuilder(args);
		pb.directory(new File("D:/test"));
		pb.redirectErrorStream(true);
		final Process process = pb.start();
	    InputStream is = process.getInputStream();
	    InputStreamReader isr = new InputStreamReader(is);
	    BufferedReader br = new BufferedReader(isr);
	    String line;
	    while ((line = br.readLine()) != null) {
	      System.out.println(line);
	    }
	   
	    System.out.println("Waited for: "+ process.waitFor());
	    System.out.println("Program terminated! ");
	}
	public static void main(String a[]){
		//hdfsMR();
		hbaseMR();
		/*try {
			 Process proc = Runtime.getRuntime().exec("java -jar F:/hadoop/mapred.jar infodata out1");
			    proc.waitFor();
			    // Then retreive the process output
			    InputStream in = proc.getInputStream();
			    InputStream err = proc.getErrorStream();

			    byte b[]=new byte[in.available()];
			    in.read(b,0,b.length);
			    System.out.println(new String(b));

			    byte c[]=new byte[err.available()];
			    err.read(c,0,c.length);
			    System.out.println(new String(c));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/ 
	}
	private static void hbaseMR() {
		String[] args = new String[]{"java","-jar","F:/hadoop/hmapred.jar"};
		try {
			//JarRunner.main(args);
			run(args);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private static void hdfsMR() {
		String[] args = new String[]{"java","-jar","F:/hadoop/mapred.jar", "infodata", "out1","defaultcombine" };
		try {
			//JarRunner.main(args);
			run(args);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
