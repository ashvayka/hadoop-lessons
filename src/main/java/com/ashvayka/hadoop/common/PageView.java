package com.ashvayka.hadoop.common;

public class PageView {
	private static final String LANG_PROJECT_DELIM = "\\.";
	private static final String DATA_DELIM = " ";
	public final String language;
	public final WikiProject wikiProject;
	public final String pageTitle;
	public final long requestCount;
	public final long contentSize;
	
	public PageView(String language, WikiProject wikiProject, String pageTitle,
			long requestCount, long contentSize) {
		super();
		this.language = language;
		this.wikiProject = wikiProject;
		this.pageTitle = pageTitle;
		this.requestCount = requestCount;
		this.contentSize = contentSize;
	}

	public static PageView parse(String data){
		try{
			String[] tokens = data.split(DATA_DELIM);
			String[] langProjectPair = tokens[0].toLowerCase().split(LANG_PROJECT_DELIM);
			if(langProjectPair.length == 1){
				return new PageView(langProjectPair[0], WikiProject.WIKIPEDIA, 
						tokens[1], Long.parseLong(tokens[2]), Long.parseLong(tokens[3]));
			}else{
				return new PageView(langProjectPair[0], WikiProject.getByExtension(langProjectPair[1]), 
						tokens[1], Long.parseLong(tokens[2]), Long.parseLong(tokens[3]));				
			}
		}catch(Exception e){
			e.printStackTrace();
			throw new IllegalArgumentException("Can't parse data: " + data);
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((language == null) ? 0 : language.hashCode());
		result = prime * result
				+ ((pageTitle == null) ? 0 : pageTitle.hashCode());
		result = prime * result
				+ ((wikiProject == null) ? 0 : wikiProject.name().hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PageView other = (PageView) obj;
		if (language == null) {
			if (other.language != null)
				return false;
		} else if (!language.equals(other.language))
			return false;
		if (pageTitle == null) {
			if (other.pageTitle != null)
				return false;
		} else if (!pageTitle.equals(other.pageTitle))
			return false;
		if (wikiProject != other.wikiProject)
			return false;
		return true;
	}
	
	
}
