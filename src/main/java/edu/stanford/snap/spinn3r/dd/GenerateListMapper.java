package edu.stanford.snap.spinn3r.dd;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.stanford.snap.spinn3rHadoop.Search.ProcessingTime;
import edu.stanford.snap.spinn3rHadoop.utils.Spinn3rDocument;
 
public class GenerateListMapper extends
        Mapper<Object, Text, Text, Text> {
 
    private Text word = new Text();
    private Text value = new Text();
 
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
    	//Log log = LogFactory.getLog(GenerateListMapper.class);
    	
    	Spinn3rDocument d = new Spinn3rDocument(value.toString());
    	
    	Text docId = new Text(d.docId);
    	String host = getDomainName(d.urlString);
    	  	
    	this.write(context, "C", new String[]{d.content}, docId);
    	this.write(context, "TC", new String[]{d.title, d.content}, docId);
    	
    	this.write(context, "HT", new String[]{host, d.title}, docId);
    	this.write(context, "HC", new String[]{host, d.content}, docId);
    	this.write(context, "HTC", new String[]{host, d.title, d.content}, docId);
    	
    	this.write(context, "U", new String[]{host}, docId);
    	this.write(context, "UT", new String[]{host, d.title}, docId);
    	this.write(context, "UC", new String[]{host, d.content}, docId);
    	this.write(context, "UTC", new String[]{host, d.title, d.content}, docId);
    }
    
    private void write(Context c, String type, String[] parts, Text docId) throws IOException, InterruptedException {
    	word.set(type + "+" + getHash(StringUtils.join(parts, "")));
    	c.write(word, docId); 
    }
    
    private static String getHash(String input) {
    	MessageDigest md;
		try {
			md = MessageDigest.getInstance("SHA-256");
			md.update(input.getBytes());
			 
	        byte byteData[] = md.digest();
	 
	        //convert the byte to hex format method 1
	        StringBuffer sb = new StringBuffer();
	        for (int i = 0; i < byteData.length; i++) {
	        	sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
	        }
	        return sb.toString();
		} catch (Exception e) {}
		
		
		return null;
        
    }
    
    private static String getDomainName(String url){
	    URI uri;
		try {
			uri = new URI(url);
			String domain = uri.getHost();
		    return domain.startsWith("www.") ? domain.substring(4) : domain;
		} catch (Exception e) {}
		
	    return url;
	}
}
