package PageRank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class linksReducer extends MapReduceBase implements Reducer <Text, Text, Text, Text> 
{
	public void reduce(Text key, Iterator<Text> values,OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
	{	
		ArrayList<String> allLinks = new ArrayList<String>();
		boolean flag = false;
		String key_in = key.toString();
		if(key_in.equals("<empty>"))
		{	
			while(values.hasNext())
			{	
				String val = values.next().toString();
				output.collect(new Text(val), new Text("<empty>"));
			}
		}
		else
		{
			while (values.hasNext()) 	
			{	
			
				String inlink = values.next().toString();			
				if(inlink.contains("!redlink"))
				{
					flag = true;	
				}
				else
				{
					allLinks.add(inlink);
				}
			}	

		
			if(flag != true)
			{
			return;
			}
			else
			{	//change
				if(allLinks.isEmpty())
				{
					output.collect(key, new Text("<empty>"));
				}
				else
				{
					for(String s : allLinks)
					{
						output.collect(new Text(s), key);
					}
				}
			}
		}
	}
}