package mapred.kmeans;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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

public class KmeansMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
    
    private HashMap<String,ArrayList<Float>> meansCluster= new HashMap<String,ArrayList<Float>>();
    
    public void map(LongWritable key, Text value, Context output, Reporter reporter) throws IOException, InterruptedException {
    	System.out.println("map this");
    	//setto arraylist coordinate    
      ArrayList<String> coordinate= new ArrayList<String>();
      StringTokenizer itr = new StringTokenizer(value.toString());
      String key_add=itr.nextElement().toString();
     
      double[] array = new double[25];
      int k=0;
      
      Text word= new Text(key_add);
      Text out= new Text();
      FloatWritable f= new FloatWritable();
       
      while (itr.hasMoreTokens()) {
          coordinate.add(itr.nextToken().toString());
          //output.write(new Text(word),new Text(itr.nextToken().toString()));
          //ff.set(new Float(itr.nextToken().toString()));
          
        //  vw.set(itr.nextToken().toString()));
          array[k]=Double.parseDouble(itr.nextToken().toString());
          out.set(itr.nextToken().toString());
          f.set(Float.parseFloat(itr.nextToken().toString()));
          output.write(new Text(out),f );
          k++;
      }
      
      //System.out.println("Key: "+ key_add + "Coord: "+ coordinate.toString());
      double a = PointToNearestCluster(array);
      System.out.println("Distance " + a);
    }
    
   @Override
   public void setup(Context context){
	   System.out.println("setup");
	   try {
		   ByteArrayOutputStream byte1=new ByteArrayOutputStream();
	       PrintStream out2 = new PrintStream(byte1);
	             
	       String uri = "/user/unicondor/cluster/cluster";
	       Configuration conf = new Configuration();
	       FileSystem fs = FileSystem.get(conf);
	       fs.setDefaultUri(conf, uri);
	       FSDataInputStream in = null;
	       try {
	    	   in = fs.open(new Path(uri));
	    	   IOUtils.copyBytes(in, out2, 4096, false);
	    	   String s=byte1.toString();
	        
	    	   String lines[]= s.split("\n");
	    	   for(int i=0; i<lines.length; i++){
	    		   ArrayList<Float> centers = new ArrayList<Float>();
	    		   StringTokenizer itr = new StringTokenizer(lines[i]);
	    		   String id_cluster= itr.nextElement().toString();
	                    
		            while(itr.hasMoreElements()){
		            	centers.add(Float.parseFloat(itr.nextElement().toString()));
		            }
	            
		            meansCluster.put(id_cluster, centers);
	    	   }
	        
	    	   //DEBUG
		        Iterator iter=meansCluster.keySet().iterator();
		        while (iter.hasNext()) {  
		        	String key = iter.next().toString();  
		            String value = meansCluster.get(key).toString();  
		                		   
		            System.out.println("CLUSTER: "+ key + " " + value);  
		            System.out.println();
		        }  
	       } 
	       finally {
	                IOUtils.closeStream(in);
	       }
	   } catch (IOException e) {
		   System.out.println("exception thrown");
		   e.printStackTrace();
	   }
   }
   
   public double PointToNearestCluster(double[] coord_point) throws IOException {
	   System.out.println("point to nearest");
	       Iterator iter=meansCluster.keySet().iterator(); 
	       double nearestDistance = Double.MAX_VALUE;
	       double[] array_cluster = new double[25];
	       int i=0;
	       while (iter.hasNext()) {  
	    	   
	    	   String key = iter.next().toString();
		      array_cluster[i]= Double.parseDouble( meansCluster.get(key).toString());
	       }
	       double distance = KmeansUtil.getEuclideanDistance( array_cluster, coord_point);
	       return distance;
   }

}