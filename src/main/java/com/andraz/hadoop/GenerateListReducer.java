package com.andraz.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
 
public class GenerateListReducer extends
        Reducer<Text, Text, NullWritable, Text> {
 
	private MultipleOutputs mos;
	
	public void setup(Context context) {
		 mos = new MultipleOutputs(context);
	}
	
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
    	String outputPath = context.getConfiguration().getStrings("args")[4];
    	String[] keySplited = key.toString().split("\\+");
    	
    	Log log = LogFactory.getLog(GenerateListReducer.class);
    	int sum = 0;
    	List<String> docIds = new ArrayList<String>();
    	
        for (Text value : values) {
            docIds.add(value.toString());
        }
        Collections.sort(docIds);
        
        if (Math.random() < 0.0001 && docIds.size() < 10){
	        log.info(docIds.toString());
        }
        Text output = new Text();
        output.set(docIds.get(0));
        //context.write(NullWritable.get(), output);
        mos.write("Output", NullWritable.get(), output, outputPath + keySplited[0] + "/");
    }
    
    public void cleanup(Context c) throws IOException, InterruptedException {
    	mos.close();
    }
}
