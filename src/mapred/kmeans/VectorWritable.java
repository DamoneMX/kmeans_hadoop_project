package mapred.kmeans;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Vector;

public class VectorWritable extends Configured implements Writable {

  private Vector vector;

  public Vector get() {
    return vector;
  }

  public void set(Vector vector) {
    this.vector = vector;
  }

  public VectorWritable() {
  }

  public VectorWritable(Vector v) {
    vector = v;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Writable w;
    if (vector instanceof Writable) {
      w = (Writable) vector;
    
    w.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    try {
      String vectorClassName = in.readUTF();
      Class<? extends Vector> vectorClass = Class.forName(vectorClassName).asSubclass(Vector.class);
      vector = vectorClass.newInstance();
      ((Writable)vector).readFields(in);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    } catch (ClassCastException cce) {
      throw new IOException(cce);
    } catch (InstantiationException ie) {
      throw new IOException(ie);
    } catch (IllegalAccessException iae) {
      throw new IOException(iae);
    }
  }

  /** Write the vector to the output */
  public static void writeVector(DataOutput out, Vector vector) throws IOException {
    new VectorWritable(vector).write(out);
  }

}
