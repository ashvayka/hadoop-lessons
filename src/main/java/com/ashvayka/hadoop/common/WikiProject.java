package com.ashvayka.hadoop.common;

public enum WikiProject {
	WIKIPEDIA(""), 
	WIKIBOOKS("b"), WIKTIONARY("d"), WIKIMEDIA("m"), 
	WIKIPEDIA_MOBILE("mw"), WIKINEWS("n"), WIKIQUOTE("q"), 
	WIKISOURCE("s"), WIKIVERSITY("v"), MEDIAWIKI("w");
	
	private final String extension;
	
	private WikiProject(String extension){
		this.extension = extension;
	}
	
	public String getExtension(){
		return extension;
	};
	
	public static WikiProject getByExtension(String extension){
		for(WikiProject project : WikiProject.values()){
			if(project.extension.equals(extension)){
				return project;
			}
		}
		throw new IllegalArgumentException("There is no project with extension: " + extension);
	}
}
