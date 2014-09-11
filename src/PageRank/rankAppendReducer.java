package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class rankAppendReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> 
{
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException 
	{
		String value = "";
		boolean first = true;
		while (values.hasNext()) 
		{
			if(!first)
			{
				value += "\t";
			}
			value += values.next().toString() ;
			first = false; 	
		}
		output.collect(key, new Text(value));
	}
}
