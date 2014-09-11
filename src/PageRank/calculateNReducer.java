package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;

public class calculateNReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> 
{
	private Long InputPageCounter;
	
	public void configure(JobConf conf)
	{
		try 
		{
			JobClient client;
			client = new JobClient(conf);
			RunningJob parentJob = client.getJob(JobID.forName(conf.get("mapred.job.id")));
			InputPageCounter = parentJob.getCounters().getCounter(PageRank.Counters.INPUT_TITLES);
		
		} catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException 
	{
		
			String value = values.next().toString();
			output.collect(key, new Text(InputPageCounter + "\t"  + value  ));		
	}
}
