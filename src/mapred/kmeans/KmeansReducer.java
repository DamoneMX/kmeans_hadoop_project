package mapred.kmeans;
import java.io.IOException;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.fs.*;
import mapred.util.Tokenizer;

public class KmeansReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	 
  public HashMap<Long, Vector> clustersMap = new HashMap<Long, Vector>();

  @Override
  public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    //System.out.println("-- inside reducer function: reduce");
    double sumx  = 0;
    double sumy  = 0;
    long   count = 0; 
    for (Text v : values) {      
      // value format: clusterId \t pointId, name, longitude, latitude
      String [] token = Tokenizer.tokenize(v.toString(), ",");
      sumx  = sumx + Double.parseDouble(token[1]);
      sumy  = sumy + Double.parseDouble(token[2]);
      count++;
      
      int index = v.toString().indexOf('\t');
      Text newVal = new Text(v.toString().substring(index+1));
      context.write(key, newVal);
    }
    sumx = sumx / count;
    sumy = sumy / count;
    Vector xy = new Vector();
    xy.add(new Double(sumx));
    xy.add(new Double(sumy));
    clustersMap.put(new Long(key.get()), xy);
  }

  @Override
  // write the centers to files
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    Configuration cfg = context.getConfiguration();
    String path   = cfg.get("currBase") + "/" + cfg.get("clustersDir") + "/" + context.getTaskAttemptID().toString();
    Path outPath  = new Path(path);
    FileSystem fs = FileSystem.get(cfg);
    fs.delete(outPath, true);
    try (SequenceFile.Writer out = SequenceFile.createWriter(fs, cfg, outPath, 
                                                             LongWritable.class,
                                                             Text.class)) {

      Iterator it = clustersMap.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<Long, Vector> pair = (Map.Entry<Long, Vector>) it.next();
        String val = ((Double)pair.getValue().get(0)).toString() + " " +
                     ((Double)pair.getValue().get(1)).toString();
        out.append(new LongWritable(pair.getKey()), new Text(val));
      }
    }
  }
}
