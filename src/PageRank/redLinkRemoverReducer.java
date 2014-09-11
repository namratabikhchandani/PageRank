package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class redLinkRemoverReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> 
{
	public void reduce(Text key, Iterator<Text> values,OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
	{
		String value = "";
		boolean first = true;
		while (values.hasNext()) 
		{
			String val=values.next().toString();
			if(!first)
			{
				value += "\t";
			}
			if(val.equals("<empty>"))
			{
				value += "";
				//System.out.println("Removing Empty for "+key.toString());
			}
			else
			{
				value += val;
			}
			first = false; 	
		}
		
		output.collect(key, new Text(value));
	}
}
