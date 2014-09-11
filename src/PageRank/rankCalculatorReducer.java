package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class rankCalculatorReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
{
	private static final double damping = 0.85;
	
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> out, Reporter reporter) throws IOException 
	{
        boolean isExistingWikiPage = false;
        String[] split;
        double sumShareOtherPageRanks = 0;
        String links = "";
        String pageWithNandRank;
        double N =0;
 
        while(values.hasNext())
        {
        	pageWithNandRank = values.next().toString();
            if(pageWithNandRank.contains("<check_tag>")) 
            {
                isExistingWikiPage = true;
                String[] temp = pageWithNandRank.split("\\t");
                N = Double.valueOf(temp[1]);
                continue;
            }
 
            if(pageWithNandRank.startsWith("|"))
            {
                links = pageWithNandRank.substring(1);
                continue;
            }
            
            split = pageWithNandRank.split("\\t");


            double rank = Double.valueOf(split[2]);
            int countOutLinks = Integer.valueOf(split[3]);
 
            sumShareOtherPageRanks += (rank/countOutLinks);
        }
 
        if(!isExistingWikiPage) 
        	return;
		
        double newRank = damping * sumShareOtherPageRanks + (1-damping)/N;
        out.collect(key, new Text(N + "\t" + newRank + "\t" + links));
    }
}