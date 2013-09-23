package com.ashvayka.hadoop.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ashvayka.hadoop.common.LangProjectKey;
import com.ashvayka.hadoop.common.PageView;
import com.ashvayka.hadoop.common.PageViewParseException;

public class LanguageProjectPageCountJob extends AbstractPageCountJob{
	
	private static final String OUTPUT_FOLDER_NAME = "lang_project_page_count";

	public static final Logger logger = LoggerFactory.getLogger(LanguageProjectPageCountJob.class);

	private Path inputPath;
	
	public LanguageProjectPageCountJob(Path inputPath) {
		super();
		this.inputPath = inputPath;
	}

	public static void main(String[] args) throws Exception {
		logger.info("Starting " + LanguageProjectPageCountJob.class.getSimpleName() + " job");
		ToolRunner.run(new Configuration(), new LanguageProjectPageCountJob(getInputPath(args)), args);
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		
		Job job = new Job(getConf(), "LanguageProjectPageCountJob");

		job.setJarByClass(LanguageProjectPageCountJob.class);
		
		job.setMapOutputKeyClass(LangProjectKey.class);
		job.setMapOutputValueClass(LongWritable.class);		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapperClass(LanguagePageCountMapper.class);
		job.setReducerClass(LanguagePageCountReducer.class);
		job.setCombinerClass(LanguagePageCountReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, getOutputPath());
		
		job.submit();
		
		return (job.waitForCompletion(true)) ? 1 : 0;
	}	
	
	public static class LanguagePageCountMapper extends Mapper<LongWritable, Text, LangProjectKey, LongWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			try{
				PageView pageView = PageView.parse(value.toString());
				context.write(new LangProjectKey(pageView.language, pageView.wikiProject), new LongWritable(pageView.requestCount));				
			}catch(PageViewParseException ex){
				logger.error("PageView parse failed", ex);
			}
			context.progress();
		} 
			
	}
	
	public static class LanguagePageCountReducer extends Reducer<LangProjectKey, LongWritable, Text, LongWritable>{
		
		@Override 
		protected void reduce(LangProjectKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			long totalRequestCount = 0;
			for(LongWritable requestCount: values){
				totalRequestCount += requestCount.get();
			}
			context.write(new Text(key.toString()), new LongWritable(totalRequestCount));
			context.progress();
		}
	}

	@Override
	String getOutputFolderName() {
		return OUTPUT_FOLDER_NAME;
	}
		
}
