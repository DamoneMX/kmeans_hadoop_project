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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.fs.*;

import mapred.util.Tokenizer;
import mapred.filesystem.CommonFileOperations;


public class KmeansMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    
  private HashMap<Long, Vector> clustersMap = new HashMap<Long, Vector>();

  @Override
  public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

    //System.out.println("-- inside mapper function : map");  
    //System.out.println("-- value: " + value);

    // input text clusterId \t pointId,x,y 
    String [] token = Tokenizer.tokenize(value.toString(), ",");
    String [] tt    = Tokenizer.tokenize(token[0],"\t");
    //for (int i=0; i<token.length; ++i) {
    //  System.out.println(" -- " + token[i]);
    //}
    LongWritable currClusterId = new LongWritable(Long.parseLong(tt[0]));
    LongWritable newClusterId  = getNearestCluster(Double.parseDouble(token[1]),
                                                   Double.parseDouble(token[2]));
    //if (currClusterId.get() != newClusterId.get()) {
    //  System.out.println(" -- found change:" + currClusterId.toString() + " " + newClusterId.toString());      
    //}
    output.write(newClusterId, value);
  }
    
  @Override
  public void setup(Context context) throws IOException, InterruptedException {

    //read all cluster files and store them the clusters map
    //System.out.println(" -- inside mapper function: setup");
    String currClusters = context.getConfiguration().get("currClusters");
    //read all sequence files in that directory
    
    super.setup(context);
    Configuration cfg = context.getConfiguration();
    String clustersPath = cfg.get("preBase") + "/" + cfg.get("clustersDir");
    String [] currClusterFiles = CommonFileOperations.listAllFiles(clustersPath, null, false, new String());
    FileSystem fs = FileSystem.get(cfg);
    for (int i = 0; i < currClusterFiles.length; ++i) {  
      Path centroidFile = new Path(currClusterFiles[i]);
      try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroidFile, cfg)) {
        LongWritable key   = new LongWritable();
        Text         value = new Text();
        while (reader.next(key, value)) {
          String [] numbers = Tokenizer.tokenize(value.toString()," ");
          Vector vec = new Vector();
          vec.add(Double.parseDouble(numbers[0]));
          vec.add(Double.parseDouble(numbers[1]));
          clustersMap.put(new Long(key.get()), vec);
        }
      }
    }
    
    //DEBUG
    Iterator iter = clustersMap.entrySet().iterator(); 
    while (iter.hasNext()) {             
      Map.Entry<Long, Vector> pair = (Map.Entry<Long, Vector>) iter.next();
      Vector xy = pair.getValue();
      Double x  = (Double) xy.get(0);
      Double y  = (Double) xy.get(1);
      System.out.println("CLUSTER: "+ pair.getKey().toString() + " " + x.toString() + " " + y.toString());  
    }


  }
   
  public LongWritable getNearestCluster(Double x, Double y) throws IOException {
    //System.out.println("-- inside mapper function: PointToNearestCluster");
    Iterator iter = clustersMap.entrySet().iterator(); 
    double distance = Double.MAX_VALUE;
    Long clusterId = new Long(-1);
    while (iter.hasNext()) {             
      Map.Entry<Long, Vector> pair = (Map.Entry<Long, Vector>) iter.next();
      Vector xy = pair.getValue();
      double xc = ((Double) xy.get(0)).doubleValue();
      double yc = ((Double) xy.get(1)).doubleValue();
      double test = (x-xc) * (x-xc) + (y-yc) * (y-yc);
      if (test < distance) {
        distance = test;
        clusterId = pair.getKey();
      }
    }
    return new LongWritable(clusterId);
  }
}
