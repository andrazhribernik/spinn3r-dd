package com.andraz.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;
import java.util.Date;

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
    	Log log = LogFactory.getLog(GenerateListMapper.class);
    	
		Spinn3rDocument d = new Spinn3rDocument(value.toString());
		
		String paramValue = context.getConfiguration().getStrings("args")[5];
		String queryWord = null;
		if (paramValue.equals("content")) {
			queryWord = d.content;
		}
		else if (paramValue.equals("url")) {
			queryWord = d.urlString;
		}
		else if (paramValue.equals("url-host-only")) {
			log.info("String url: " + d.urlString);
			try {
				queryWord = getDomainName(d.urlString);
			} catch (URISyntaxException e) {
				queryWord = d.urlString;
				log.info("url host only exception: " + queryWord);
			} catch(Exception e) {
				log.info("Crashed mapper");
				queryWord = d.urlString;
			}
		}
		else {
			queryWord = d.title;
		}
		
		if (queryWord != null) {
			try {
				word.set(getHash(queryWord));
				value.set("1\t" + d.docId);
				context.write(word, value);
			} catch (NoSuchAlgorithmException e) {}
		}
    }
    
    private static String getHash(String input) throws NoSuchAlgorithmException {
    	MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(input.getBytes());
 
        byte byteData[] = md.digest();
 
        //convert the byte to hex format method 1
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < byteData.length; i++) {
         sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }
    
    private static String getDomainName(String url) throws URISyntaxException {
	    URI uri = new URI(url);
	    String domain = uri.getHost();
	    return domain.startsWith("www.") ? domain.substring(4) : domain;
	}
}
