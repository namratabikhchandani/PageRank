package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class calculateNMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
{

	public void map(LongWritable key, Text value,OutputCollector<Text,Text> output, Reporter reporter) throws IOException 
	{
		int pageTabIndex = value.find("\t");
		String page = Text.decode(value.getBytes(), 0, pageTabIndex);
        
        String restLinks = Text.decode(value.getBytes(),pageTabIndex+1, value.getLength()-(pageTabIndex+1));
        output.collect(new Text(page), new Text(restLinks));		
        
        reporter.incrCounter(PageRank.Counters.INPUT_TITLES, 1);
	}
}
