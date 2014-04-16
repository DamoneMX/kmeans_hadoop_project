package mapred.kmeans;

//import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;
import mapred.filesystem.CommonFileOperations;

public class KmeansDriver {

  public static void main(String[] args) throws Exception {
    System.out.println(" -- inside the driver main function");
   
    // get the input and output file 
    SimpleParser parser  = new SimpleParser(args);
    String input         = parser.get("input");
    String output        = parser.get("output");
    int numberOfClusters = parser.getInt("cn");
    
    System.out.println(" -- ARGS ::" + 
                       " Input: "    + input  + 
                       " Output: "   + output +
                       " clusters Num: " + numberOfClusters);

    // Iteration 0 First we need to assign clusters to all points and create clusters files
    Configuration cfg  = new Configuration();
    cfg.setInt("numberOfClusters", numberOfClusters);
    cfg.set("currBase", output + "/iteration_0");
    cfg.set("pointsDir", "points");
    cfg.set("clustersDir", "clusters");

    String currOutput = cfg.get("currBase") + "/" + cfg.get("pointsDir");
    Optimizedjob initJob = new Optimizedjob(cfg, input, currOutput, "initialize kmeans");  
    initJob.setClasses(KmeansInitMapper.class, KmeansInitReducer.class, null);
    initJob.setMapOutputClasses(LongWritable.class, Text.class);
    initJob.run();    

    // now foreach iteration we need to update the points to cluster map
    // cluster centers also should be updated 
    int iteration = 1;

    while (iteration < 10) {
      // update file pathes 
      cfg.set("preBase", cfg.get("currBase"));
      cfg.set("currBase", output + "/iteration_" + iteration);
     
      String currInputs = cfg.get("preBase")  + "/" + cfg.get("pointsDir");
      currOutput = cfg.get("currBase") + "/" + cfg.get("pointsDir");
      String [] currInputFiles = CommonFileOperations.listAllFiles(currInputs, null, false, "_SUCCESS");
      System.out.println(" -- input file:" + currInputFiles[0]);
      Optimizedjob job = new Optimizedjob(cfg, currInputFiles[0], currOutput, "kmeans iteration");
      for (int i=1; i<currInputFiles.length; ++i) {
        System.out.println(" -- input file:" + currInputFiles[i]);
        job.addInput(currInputFiles[i]);
      }
      // create a new job using kmeans mapper and reducer
      job.setClasses(KmeansMapper.class, KmeansReducer.class, null);
      job.setMapOutputClasses(LongWritable.class, Text.class);
      job.run();
      iteration++;
    }
  }
}



