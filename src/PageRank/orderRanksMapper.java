package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class orderRanksMapper extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {

    public void map(LongWritable key, Text value, OutputCollector< DoubleWritable, Text> output, Reporter arg3) throws IOException {
    
    	  String[] all = Text.decode(value.getBytes(), 0, value.getLength()).split("\t", 4);
    	  String page = all[0];
    	  Double N = Double.parseDouble(all[1]);
    	  Double rank = Double.parseDouble(all[2]);
    	  
    	  if(rank > 5.0/N) 
    	  {
    		  output.collect(new DoubleWritable(rank), new Text(page));
    	  }
    } 
}
