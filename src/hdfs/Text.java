package hdfs;

import java.io.Serializable;

import mapreduce.hi.api.Notused;
@Notused
public class Text implements Serializable{
	public String name;
	public String type;
	public Text(String name, String type) {
		super();
		this.name = name;
		this.type = type;
	}
	@Override
	public String toString() {
		return "*********  Name : "+this.name+" , Type : "+this.type+" ************";
	}
}
