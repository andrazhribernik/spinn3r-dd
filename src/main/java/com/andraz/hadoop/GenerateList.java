package com.andraz.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
public class GenerateList extends Configured implements Tool {
 
    public static void main(String[] args) throws Exception,
            InterruptedException, ClassNotFoundException {
 
        // Create configuration
        Configuration conf = new Configuration(true);
        conf.set("textinputformat.record.delimiter","\n\n");
        conf.setStrings("args", args);
 
        // Create job
        int res = ToolRunner.run(conf, new GenerateList(), args);
        System.exit(res);
 
    }
    
    public final int run(final String[] args) throws Exception {
    	Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        
    	Job job = new Job(super.getConf(), "Generate lists of original news");
        job.setJarByClass(GenerateListMapper.class);
 
        // Setup MapReduce
        job.setMapperClass(GenerateListMapper.class);
        job.setReducerClass(GenerateListReducer.class);
        //job.setNumReduceTasks(1);
 
        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);
 
        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "Output", TextOutputFormat.class, NullWritable.class, Text.class);
 
        // Delete output if exists
        FileSystem hdfs = FileSystem.get(super.getConf());
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);
 
        // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;
        return 0;
    }
 
}
