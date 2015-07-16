package com.andraz.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class GenerateListReducer extends
        Reducer<Text, Text, Text, Text> {
 
    public void reduce(Text text, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
    	int sum = 0;
    	List<String> docIds = new ArrayList<String>();
    	
        for (Text value : values) {
        	String[] valueSplited = value.toString().split("\t");
            sum += Integer.parseInt(valueSplited[0]);
            docIds.add(valueSplited[1]);
        }
        if (docIds.size() > 1){
	        Text output = new Text();
	        output.set(	sum + "\t" + 
	        			docIds.toString()
					     .replace("[", "")
					     .replace("]", "")
					     .replace(", ", ","));
	        context.write(text, output);
        }
    }
}
