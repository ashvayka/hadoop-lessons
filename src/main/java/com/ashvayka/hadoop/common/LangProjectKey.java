package com.ashvayka.hadoop.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class LangProjectKey implements WritableComparable<LangProjectKey>{

	String language;
	WikiProject project;
	
	public LangProjectKey(String language, WikiProject project) {
		super();
		this.language = language;
		this.project = project;
	}

	@Override
	public void readFields(DataInput input) throws IOException {		
		language = input.readUTF();
		project = WikiProject.getByExtension(input.readUTF());
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(language);
		output.writeUTF(project.getExtension());
	}

	@Override
	public String toString() {
		return "LangProjectKey [language=" + language + ", project=" + project + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((language == null) ? 0 : language.hashCode());
		result = prime * result + ((project == null) ? 0 : project.ordinal());
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
		LangProjectKey other = (LangProjectKey) obj;
		if (language == null) {
			if (other.language != null)
				return false;
		} else if (!language.equals(other.language))
			return false;
		if (project != other.project)
			return false;
		return true;
	}

	@Override
	public int compareTo(LangProjectKey other) {
		int tmp = language.compareTo(other.language);
		
		if(tmp != 0){
			return tmp;
		}
		
		return project.compareTo(other.project);
	}
}