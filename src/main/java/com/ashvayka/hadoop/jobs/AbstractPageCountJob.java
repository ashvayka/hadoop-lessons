package com.ashvayka.hadoop.jobs;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

public abstract class AbstractPageCountJob extends Configured implements Tool{

	private static final String DEFAULT_INPUT_PATH = "/wikidata/pageviews/*";
	private static final String BASE_OUTPUT_PATH = "/wikidata/output/";
	
	abstract String getOutputFolderName();
	
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
