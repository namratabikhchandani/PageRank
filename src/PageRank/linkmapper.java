package PageRank;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.*;


public class linkmapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	//static enum Counters { INPUT_WORDS }
	
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
	{   
        String file_contents = value.toString();

        int start = value.find("<title>");
        int end = value.find("</title>", start);
         
        String title = file_contents.substring(start+7, end);
       
        	title = title.replace(' ', '_');
        	output.collect(new Text(title), new Text("!redlink"));
        
        	int start_text = value.find("<text");
        	int temp = value.find(">", start_text);
        	int end_text = value.find("</text>");
                
        	String text = new String();
        	try
        	{
        		text = Text.decode(value.getBytes(), temp, end_text-temp);
        	}catch(Exception e)
        	{
        		text=null;
        	}

        	if(text!=null)
        	{
	        	Pattern text_pattern = Pattern.compile("\\[\\[(?:[^|\\]]*\\|)?([^\\]]+)\\]\\]");
	        	Matcher text_match = text_pattern.matcher(text);
	                  
	        	String weblink = "";
	        	HashSet<String> hs = new HashSet<String>();
	        	while(text_match.find()) 
	        	{
	               int start_tag = text_match.start();
	               int end_tag = text_match.end();
	               
	               weblink =  text.substring(start_tag + 2, end_tag- 2);  
	            
	               if(weblink.contains("|"))
	               {
	            	   int index = weblink.indexOf('|');
	            	   weblink = weblink.substring(0, index);
	               }
	               
	                          
	          	   weblink = weblink.replace(' ', '_');
	           	   weblink = weblink.replace("&amp;", "&");
	           	   hs.add(weblink);
	           	}
	        
	        	if(hs.isEmpty())
	        	{
	        		output.collect(new Text("<empty>"), new Text(title));
	        	}
	        
	        	Iterator<String> iterator = hs.iterator();
	        	
	        	while(iterator.hasNext())
	        	{	
	        		output.collect(new Text(iterator.next()),new Text(title));       	    	
	        	}
        	}
        	else
        	{
        		output.collect(new Text("<empty>"), new Text(title));
        	}
	}
}	