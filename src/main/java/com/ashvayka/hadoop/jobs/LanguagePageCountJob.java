package com.ashvayka.hadoop.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ashvayka.hadoop.common.PageView;
import com.ashvayka.hadoop.common.PageViewParseException;

public class LanguagePageCountJob extends AbstractPageCountJob{
	
	private static final String JOB_NAME = "LanguagePageCountJob";

	private static final String OUTPUT_FOLDER_NAME = "lang_page_count";

	public static final Logger logger = LoggerFactory.getLogger(LanguagePageCountJob.class);
	
	public LanguagePageCountJob(Path inputPath) {
		super(inputPath);
	}

	public static void main(String[] args) throws Exception {
		logger.info("Starting " + LanguagePageCountJob.class.getSimpleName() + " job");
		ToolRunner.run(new Configuration(), new LanguagePageCountJob(getInputPath(args)), args);
	}
	
	@Override
	protected void setupJobParams(Job job) {
		//Map Key Value classes		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);		
		//Reduce Key Value classes		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapperClass(LanguagePageCountMapper.class);
		job.setReducerClass(LanguagePageCountReducer.class);
		job.setCombinerClass(LanguagePageCountReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(4);
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

	@Override
	String getOutputFolderName() {
		return OUTPUT_FOLDER_NAME;
	}

	@Override
	String getJobName() {
		return JOB_NAME;
	}
		
}
