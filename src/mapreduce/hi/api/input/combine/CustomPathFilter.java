package mapreduce.hi.api.input.combine;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

class CustomPathFilter implements PathFilter{
	@Override
	public boolean accept(Path path) {
		 return path.toString().contains("infodata");
	}
}