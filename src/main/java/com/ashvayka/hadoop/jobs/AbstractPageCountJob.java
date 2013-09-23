package com.ashvayka.hadoop.jobs;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public abstract class AbstractPageCountJob extends Configured implements Tool{

	private static final String DEFAULT_INPUT_PATH = "/wikidata/pageviews/*";
	private static final String BASE_OUTPUT_PATH = "/wikidata/output/";
	
	private Path inputPath;
	
	abstract String getOutputFolderName();
	abstract String getJobName();
	abstract void setupJobParams(Job job);
	
	public AbstractPageCountJob(Path inputPath) {
		super();
		this.inputPath = inputPath;
	}	
	
	@Override
	public int run(String[] arg0) throws Exception {
		
		Job job = new Job(getConf(), getJobName());

		job.setJarByClass(AbstractPageCountJob.class);
		
		setupJobParams(job);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, getOutputPath());
		
		job.submit();
		
		return (job.waitForCompletion(true)) ? 1 : 0;
	}
	
	protected Path getOutputPath() throws IOException{
		FileSystem fs = FileSystem.get(getConf());
		Path path = new Path(getOutputFolderPath());
		if(fs.exists(path)){
			fs.rename(path, new Path(getOutputFolderPath() + new Date().getTime()));
		};
		return path;
	}		
	
	protected static Path getInputPath(String[] args) {
		return new Path(args.length > 0 ? args[0] : DEFAULT_INPUT_PATH);
	}
	
	private String getOutputFolderPath(){
		return BASE_OUTPUT_PATH + getOutputFolderName();
	};	
	
}
