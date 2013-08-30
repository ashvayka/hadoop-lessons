package com.ashvayka.hadoop.jobs;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ashvayka.hadoop.common.PageView;
import com.ashvayka.hadoop.common.PageViewParseException;

public class LanguagePageCountJob extends Configured implements Tool{
	
	private static final String DEFAULT_INPUT_PATH = "/wikidata/pageviews/*";

	public static final Logger logger = LoggerFactory.getLogger(LanguagePageCountJob.class);

	private String inputPath;
	
	public LanguagePageCountJob(String inputPath) {
		super();
		this.inputPath = inputPath;
	}

	public static void main(String[] args) throws Exception {
		logger.info("Starting " + LanguagePageCountJob.class.getSimpleName() + " job");
		ToolRunner.run(new Configuration(), new LanguagePageCountJob(getInputPath(args)), args);
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		
		Job job = new Job(getConf(), "LanguagePageCountJob");

		job.setJarByClass(LanguagePageCountJob.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapperClass(LanguagePageCountMapper.class);
		job.setReducerClass(LanguagePageCountReducer.class);
		job.setCombinerClass(LanguagePageCountReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, getOutputPath());
		
		job.submit();
		
		return (job.waitForCompletion(true)) ? 1 : 0;
	}
		
	private Path getOutputPath() throws IOException{
		FileSystem fs = FileSystem.get(getConf());
		Path path = new Path("/wikidata/output/language_page_count");
		if(fs.exists(path)){
			fs.rename(path, new Path("/wikidata/output/language_page_count" + new Date().getTime()));
		};
		return path;
	}
	
	
	public static class LanguagePageCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			try{
				PageView pageView = PageView.parse(value.toString());
				context.write(new Text(pageView.language), new LongWritable(pageView.requestCount));				
			}catch(PageViewParseException ex){
				logger.error("PageView parse failed", ex);
			}
			context.progress();
		} 
			
	}
	
	public static class LanguagePageCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		
		@Override 
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			long totalRequestCount = 0;
			for(LongWritable requestCount: values){
				totalRequestCount += requestCount.get();
			}
			context.write(key, new LongWritable(totalRequestCount));
			context.progress();
		}
	}
	
	private static String getInputPath(String[] args) {
		return args.length > 0 ? args[0] : DEFAULT_INPUT_PATH;
	}	
	
}
