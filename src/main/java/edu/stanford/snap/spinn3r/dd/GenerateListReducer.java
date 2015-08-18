package edu.stanford.snap.spinn3r.dd;

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
    	
    	List<String> docIds = new ArrayList<String>();
        for (Text value : values) {
            docIds.add(value.toString());
        }
        Collections.sort(docIds);
        
        Text output = new Text();
        output.set(docIds.get(0));
        
        mos.write("Output", NullWritable.get(), output, outputPath + "/" + keySplited[0] + "/");
    }
    
    public void cleanup(Context c) throws IOException, InterruptedException {
    	mos.close();
    }
}
