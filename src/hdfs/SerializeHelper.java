package hdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.fs.FSDataInputStream;

public class SerializeHelper {
	public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return b.toByteArray();
    }

    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }
    public static void serialize(String outFile, Object serializableObject)
    throws IOException {
	    FileOutputStream fos = new FileOutputStream(outFile);
	    ObjectOutputStream oos = new ObjectOutputStream(fos);
	    oos.writeObject(serializableObject);
	}
	
	public static Object deserialize(String serilizedObject)
	    throws FileNotFoundException, IOException, ClassNotFoundException {
		    FileInputStream fis = new FileInputStream(serilizedObject);
		    ObjectInputStream ois = new ObjectInputStream(fis);
		    return ois.readObject();
	}
	public static Object deserialize(FSDataInputStream in)
    throws FileNotFoundException, IOException, ClassNotFoundException {
	    ObjectInputStream ois = new ObjectInputStream(in);
	    return ois.readObject();
	}
}
