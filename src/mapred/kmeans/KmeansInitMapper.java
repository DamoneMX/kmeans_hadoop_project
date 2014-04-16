package mapred.kmeans;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KmeansInitMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

  private int numberOfClusters;

  @Override
  public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

    System.out.println("-- inside init mapper function : map");  
    System.out.println( "value: " + value);
    // now generate a random integer and assign it to this entry 
    Random rand = new Random();
    LongWritable clusterId = new LongWritable(rand.nextInt(numberOfClusters));
    output.write(clusterId, value); 
  }
    
  @Override
  public void setup(Context context) {
    System.out.println(" -- inside init mapper function: setup");
    numberOfClusters = context.getConfiguration().getInt("numberOfClusters", 0);
  }
}
