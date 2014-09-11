package PageRank;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PageRank {
	static enum Counters { INPUT_TITLES };
	public static int runs = 1;

	
	public static void main(String[] args) throws IOException 
	{
		String bucket = args[0];
		PageRank page = new PageRank();
        
		page.runXmlParsing(bucket);
        page.redLinkRemover(bucket);
        page.calculateN(bucket);
        page.emitN(bucket);
		page.appendRank(bucket);
        
        page.runRankCalculator(bucket + "/tmp/appendRank/", bucket +"/tmp/iter.21/");
        page.orderRanks(bucket +"/tmp/iter.21/", bucket +"/tmp/orderedRank.1/",bucket);
        
        for (runs =2 ; runs < 9; runs++) 
        {	
        	String path_in = bucket +"/tmp/iter.2" + (runs-1) + "/";
        	String path_out = bucket +"/tmp/iter.2" + runs + "/";
        	page.runRankCalculator(path_in, path_out);
        }
        runs--;
        page.orderRanks(bucket +"/tmp/iter.2"+runs, bucket +"/tmp/orderedRank."+runs,bucket);
	}	
	
	 
	 public void runXmlParsing(String path) throws IOException 
	 {
	        JobConf conf = new JobConf(PageRank.class);
	        conf.setJobName("Page Rank");
	        
	        conf.setJarByClass(PageRank.class);
	        FileInputFormat.setInputPaths(conf, new Path("s3n://spring-2014-ds/data/enwiki-latest-pages-articles.xml"));
	        conf.setInputFormat(XmlInputFormat.class);
	        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
	        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
	        conf.setMapperClass(linkmapper.class);
	        
	        FileOutputFormat.setOutputPath(conf, new Path(path + "/tmp/link/"));
	       
	        conf.setOutputFormat(TextOutputFormat.class);
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	        conf.setReducerClass(linksReducer.class);
	        
	        JobClient.runJob(conf);
	       
	  }
	 
	 public void redLinkRemover(String path) throws IOException 
	 {
	        JobConf conf = new JobConf(PageRank.class);
	        conf.setJobName("Page Rank");
	        
	        conf.setJarByClass(PageRank.class);
	        FileInputFormat.setInputPaths(conf, new Path(path + "/tmp/link/"));
	        conf.setInputFormat(TextInputFormat.class);
	        conf.setMapperClass(redLinkRemoverMapper.class);
	        
	        FileOutputFormat.setOutputPath(conf, new Path(path + "/tmp/redLinkRemoval/"));
	        conf.setOutputFormat(TextOutputFormat.class);
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	        conf.setReducerClass(redLinkRemoverReducer.class);
	        
	        JobClient.runJob(conf);
	        FileSystem fs;
	        try {
				fs = FileSystem.get(new URI(path+"/results/"),conf);
				FileUtil.copyMerge(fs,new Path(path + "/tmp/redLinkRemoval/"), fs ,new Path (path + "/results/PageRank.outlink.out"),false,conf,"");
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        
	  }
	 
	 public void calculateN(String path) throws IOException 
	 {
		 	JobConf conf = new JobConf(PageRank.class);
		 	conf.setJobName("Page Rank");
	        
		 	conf.setJarByClass(PageRank.class);
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	        
	        conf.setInputFormat(TextInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);
	        
	        FileInputFormat.setInputPaths(conf, new Path(path + "/tmp/redLinkRemoval/"));
	        FileOutputFormat.setOutputPath(conf, new Path(path + "/tmp/calculateN/"));
	        
	        conf.setMapperClass(calculateNMapper.class);
	        conf.setReducerClass(calculateNReducer.class);
	        
	        JobClient.runJob(conf);
	   }
	 
	 public void emitN(String path) throws IOException 
	 {
	        JobConf conf = new JobConf(PageRank.class);
	        conf.setJobName("Page Rank");
	        
	        conf.setJarByClass(PageRank.class);
	        FileInputFormat.setInputPaths(conf, new Path(path + "/tmp/calculateN/"));
	        conf.setInputFormat(TextInputFormat.class);
	        conf.setMapperClass(emitNMapper.class);
	        
	        FileOutputFormat.setOutputPath(conf, new Path(path + "/tmp/emitN/"));
	        conf.setOutputFormat(TextOutputFormat.class);
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	        conf.setReducerClass(emitNReducer.class);
	        
	        JobClient.runJob(conf);
	        
	        FileSystem fs;
			try {
				fs = FileSystem.get(new URI(path+"/results/"),conf);
				FileUtil.copyMerge(fs,new Path(path + "/tmp/emitN/"), fs,new Path (path + "/results/PageRank.n.out"),false,conf,"");
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  }
	 
	 public void appendRank(String path) throws IOException 
	 {
		 	JobConf conf = new JobConf(PageRank.class);
		 	conf.setJobName("Page Rank");
	        
		 	conf.setJarByClass(PageRank.class);
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	        
	        conf.setInputFormat(TextInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);
	        
	        FileInputFormat.setInputPaths(conf, new Path(path + "/tmp/calculateN/"));
	        FileOutputFormat.setOutputPath(conf, new Path(path + "/tmp/appendRank/"));
	        
	        conf.setMapperClass(rankAppendMapper.class);
	        conf.setReducerClass(rankAppendReducer.class);
	        
	        JobClient.runJob(conf);
	  }
	 
	  public void runRankCalculator(String path_in, String path_out) throws IOException 
	  {
	        JobConf conf = new JobConf(PageRank.class);
	        conf.setJobName("Page Rank");
	        
		 	conf.setJarByClass(PageRank.class);
		 	conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	        
	        conf.setInputFormat(TextInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);
	        
	        FileInputFormat.setInputPaths(conf, new Path(path_in));
	        FileOutputFormat.setOutputPath(conf, new Path(path_out));
	        
	        conf.setMapperClass(rankCalculatorMapper.class);
	        conf.setReducerClass(rankCalculatorReducer.class);
	        JobClient.runJob(conf);
	    }
	  
	  public void orderRanks(String path_in, String path_out,String path) throws IOException 
	  {
	        JobConf conf = new JobConf(PageRank.class);
	        conf.setJobName("Page Rank");
	        
		 	conf.setJarByClass(PageRank.class);
		 	conf.setMapOutputKeyClass(DoubleWritable.class);
		 	conf.setMapOutputValueClass(Text.class);
		 	conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(DoubleWritable.class);
	        
	        conf.setInputFormat(TextInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);
	        FileInputFormat.setInputPaths(conf, new Path(path_in));
	        FileOutputFormat.setOutputPath(conf, new Path(path_out));
	        
	        conf.setMapperClass(orderRanksMapper.class);
	        conf.setOutputKeyComparatorClass(SortKeyComparator.class);
	        conf.setReducerClass(orderRanksReducer.class);
	        conf.setNumReduceTasks(1);
	        
	        JobClient.runJob(conf);
	        
	        FileSystem fs;
			try {
				fs = FileSystem.get(new URI(path+"/results/"),conf);
				String path1 = path +"/results/PageRank.iter" + runs  + ".out";
				FileUtil.copyMerge(fs, new Path(path_out), fs, new Path(path1),false, conf, "");
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }	  
	}