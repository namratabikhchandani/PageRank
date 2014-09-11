package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class orderRanksReducer extends MapReduceBase implements Reducer<DoubleWritable, Text,Text, DoubleWritable> 
{	
	//SortKeyComparator sort = new SortKeyComparator();
	public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException 
	{	
		String value = "";
		while (values.hasNext()) 
		{
			value = values.next().toString();
			output.collect(new Text(value), key);
		}
	}

}
