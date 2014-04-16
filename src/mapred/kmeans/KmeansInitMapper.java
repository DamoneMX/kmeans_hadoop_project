package mapred.kmeans;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansInitMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

  private int numberOfClusters;

  @Override
  public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

    //System.out.println("-- inside init mapper function : map");  
    //System.out.println( "value: " + value);
    // now generate a random integer and assign it to this entry 
    Random rand = new Random();
    LongWritable clusterId = new LongWritable(rand.nextInt(numberOfClusters));
    //System.out.println("-- inside init mapper function : map. clusterId: " + clusterId.toString());  
    output.write(clusterId, value); 
  }
    
  @Override
  public void setup(Context context) {
    //System.out.println(" -- inside init mapper function: setup");
    numberOfClusters = context.getConfiguration().getInt("numberOfClusters", 0);
  }
}
