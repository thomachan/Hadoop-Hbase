package mapreduce.hi.api.input.defaultcustom;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

class DefaultPathFilter implements PathFilter{
	@Override
	public boolean accept(Path path) {
		// /user/root/infodata/1389258221000
		String[] str = path.toString().split("infodata/");
		if(str != null && str[1] != null ){
			long t = Long.parseLong(str[1]);
			if(t <= 1389067571000l)
				return true;
		}
		 return false;
	}
}