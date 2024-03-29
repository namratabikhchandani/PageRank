package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class redLinkRemoverMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, Text>
{
	public void map(LongWritable key, Text values,OutputCollector<Text,Text> output, Reporter reporter) throws IOException 
	{	
		String all_string = values.toString();
		//change
		String[] all = all_string.split("\\t",2);
		String page = all[0];
		String value = all[1];
		output.collect(new Text(page), new Text(value));		
	}
}

