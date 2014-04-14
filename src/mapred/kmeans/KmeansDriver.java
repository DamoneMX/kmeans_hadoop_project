package mapred.kmeans;
import java.io.ByteArrayOutputStream;

import java.io.IOException;

import java.io.PrintStream;

import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.Float;


//import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import mapred.util.SimpleParser;
//import org.apache.jasper.tagplugins.jstl.core.Set;

public class KmeansDriver {

	public static void main(String[] args) throws Exception {
		System.out.println("here goes");
		
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		
		System.out.println("Input: " + input + " Output: " + output);
		
		
		Configuration conf = new Configuration();
		/*
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: kmeans <in> <out>");
			System.exit(2);
		}
		*/
	  
		Job job = new Job(conf,"Kmeans");
		//mio
		job.setJobName("Kmeans_Unicondor");
		job.setJarByClass(KmeansDriver.class);
		job.setMapperClass(KmeansMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(KmeansReducer.class);
		//  job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormat(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputFormatClass(FileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
    
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}



