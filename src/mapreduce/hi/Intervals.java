package mapreduce.hi;

import java.util.Calendar;
import java.util.GregorianCalendar;

public enum Intervals {
		MINUTE(0)
		{
			@Override
			public long getTime(long time) {
				Calendar cal = new GregorianCalendar();
				cal.setTimeInMillis(time);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				return cal.getTimeInMillis();
			}
		},HOUR(0)
		{
			@Override
			public long getTime(long time) {
				Calendar cal = new GregorianCalendar();
				cal.setTimeInMillis(time);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				return cal.getTimeInMillis();
			}
		}, DAY(0)
		{
			@Override
			public long getTime(long time) {
				Calendar cal = new GregorianCalendar();
				cal.setTimeInMillis(time);
				cal.set(Calendar.HOUR, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				return cal.getTimeInMillis();
			}
		},
		MONTH(0)
		{
			@Override
			public long getTime(long time) {
				Calendar cal = new GregorianCalendar();
				cal.setTimeInMillis(time);
				cal.set(Calendar.DAY_OF_MONTH,0);
				cal.set(Calendar.HOUR, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				return cal.getTimeInMillis();
			}			
		},
		YEAR(0)
		{
			@Override
			public long getTime(long time) {
				Calendar cal = new GregorianCalendar();
				cal.setTimeInMillis(time);
				cal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
				cal.set(Calendar.MONTH,0);
				cal.set(Calendar.DAY_OF_MONTH,0);
				cal.set(Calendar.HOUR, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				return cal.getTimeInMillis();
			}
		};
		int fraction;
		Intervals(int dx){
			fraction = 0;
		}
		abstract public long getTime(long time);
		public int getFraction() {
			return fraction;
		}
		public void setFraction(int fraction) {
			this.fraction = fraction;
		}
		
}
