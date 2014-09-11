package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class emitNMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text values,OutputCollector<Text,Text> output, Reporter reporter) throws IOException 
	{
		String[] all= Text.decode(values.getBytes(), 0, values.getLength()).split("\t", 3);
		String N = all[1];
		output.collect(new Text("N="),new Text( N));
	}

}
