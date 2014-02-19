package mapreduce;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Calendar cal = new GregorianCalendar();
			
			cal.setTimeInMillis(new Date().getTime());
			System.out.println(cal.getTime());
			
			cal.set(Calendar.MILLISECOND, 0);
			cal.set(Calendar.SECOND, 0);
			
			long secMilli = cal.getTimeInMillis();
			
			cal.set(Calendar.MINUTE, 0);
			long minMilli = cal.getTimeInMillis();
			long minLeft = ((secMilli - minMilli)/(1000 * 60)) % 60;
			
			long fraction = 10l;
			cal.set(Calendar.MINUTE, (int) ((minLeft/fraction )*fraction));
			System.out.println(cal.getTimeInMillis());
			System.out.println(cal.getTime());
		
		/*String coin ="15";
		int $ = Integer.parseInt(coin);
		System.out.println($);
		int i=0;
		print(i);*/
			
		
	}
	public static void print(Object p){
		System.out.println(p);
	}

}
