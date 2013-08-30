package com.ashvayka.hadoop.common;

public class PageViewParseException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String message;
	
	public PageViewParseException(String message){
		this.message = message;
	}

	public String getMessage() {
		return message;
	}
}
