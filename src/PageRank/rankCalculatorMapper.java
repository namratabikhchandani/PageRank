package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class rankCalculatorMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
    {


    	String[] all = Text.decode(value.getBytes(), 0, value.getLength()).split("\\t", 4);
    	String page = all[0];
    	String N = all[1];
    	String rank = all[2];
    	String links = all[3];
    	
    	output.collect(new Text(page), new Text("<check_tag>" + "\t" + N));
    	
    	if(links.isEmpty())
    	{
    		return;
    	}
    		
    	
    	String[] allOtherPages = links.split("\\t");
    	int totalLinks = allOtherPages.length;
    
        for (String otherPage : allOtherPages)
        {
              Text pageRankTotalLinks = new Text(page +"\t" + N +"\t"+ rank +"\t"+ totalLinks);
               output.collect(new Text(otherPage), pageRankTotalLinks);
        }
        
     // Put the original links of the page for the reduce output
        output.collect(new Text(page), new Text("|"+links));
    }
}