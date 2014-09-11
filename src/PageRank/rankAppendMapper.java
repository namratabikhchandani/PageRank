package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class rankAppendMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value,OutputCollector<Text,Text> output, Reporter reporter) throws IOException 
	{	
	        
			String[] all = Text.decode(value.getBytes(), 0, value.getLength()).split("\t", 3);
			
			Double N = Double.parseDouble(all[1]);
			Double rank = 1.0/N;
			
			String values = all[1] + "\t" + rank.toString() + "\t" + all[2];
			output.collect(new Text(all[0]), new Text(values));	
	      
	}

}
