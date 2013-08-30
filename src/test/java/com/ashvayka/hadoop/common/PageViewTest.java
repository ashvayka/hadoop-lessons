package com.ashvayka.hadoop.common;

import org.junit.Assert;
import org.junit.Test;

public class PageViewTest {

	@Test
	public void parseTestSimple() throws PageViewParseException{
		String data = "en .an 7 284707";
		PageView pageView = PageView.parse(data);
		Assert.assertEquals(pageView.language, "en");
		Assert.assertEquals(pageView.wikiProject, WikiProject.WIKIPEDIA);
		Assert.assertEquals(pageView.pageTitle, ".an");
		Assert.assertEquals(pageView.requestCount, 7L);
		Assert.assertEquals(pageView.contentSize, 284707L);
	}
	
	@Test
	public void parseTestComplex() throws PageViewParseException{
		String data = "ua.b Special:Recherche/Andr%C3%A9_Gazut.html 7 284707";
		PageView pageView = PageView.parse(data);
		Assert.assertEquals(pageView.language, "ua");
		Assert.assertEquals(pageView.wikiProject, WikiProject.WIKIBOOKS);
		Assert.assertEquals(pageView.pageTitle, "Special:Recherche/Andr%C3%A9_Gazut.html");
		Assert.assertEquals(pageView.requestCount, 7L);
		Assert.assertEquals(pageView.contentSize, 284707L);
	}
	
	@Test(expected = PageViewParseException.class)
	public void parseTestFailure() throws PageViewParseException{
		String data = "ua.trash Special:Recherche/Andr%C3%A9_Gazut.html 7 284707";
		PageView pageView = PageView.parse(data);
		Assert.assertEquals(pageView.language, "ua");
		Assert.assertEquals(pageView.wikiProject, WikiProject.WIKIBOOKS);
		Assert.assertEquals(pageView.pageTitle, "Special:Recherche/Andr%C3%A9_Gazut.html");
		Assert.assertEquals(pageView.requestCount, 7L);
		Assert.assertEquals(pageView.contentSize, 284707L);
	}
	
}
