package mapred.kmeans;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class KmeansInitReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	  
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println("-- inside init reducer function: reduce");            	
    	for (Text v : values) {
          context.write(key, v);
    	}
    }
  }
