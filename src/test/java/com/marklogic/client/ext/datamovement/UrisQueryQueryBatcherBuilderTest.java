package com.marklogic.client.ext.datamovement;

import org.junit.Assert;
import org.junit.Test;

public class UrisQueryQueryBatcherBuilderTest extends Assert {

	private UrisQueryQueryBatcherBuilder builder = new UrisQueryQueryBatcherBuilder();

	@Test
	public void javascriptStartingWithFnSubsequence() {
		String query = "fn.subsequence(cts.uris(null, null, cts.collectionQuery('Testing')), 1, 3)";
		String newQuery = builder.wrapJavascriptIfAppropriate(query);
		assertEquals("The query should not be wrapped with cts.uris since it doesn't start with cts.", query, newQuery);
	}

	@Test
	public void javascriptStartingWithCtsUris() {
		String query = "cts.uris(null, null, cts.collectionQuery('Testing'))";
		String newQuery = builder.wrapJavascriptIfAppropriate(query);
		assertEquals("The query should not be wrapped with cts.uris since it already starts with cts.uris", query, newQuery);
	}

	@Test
	public void javascriptStartingWithCtsCollectionQuery() {
		String query = "cts.collectionQuery('Testing')";
		String newQuery = builder.wrapJavascriptIfAppropriate(query);
		assertEquals("The query should not be wrapped with cts.uris since it doesn't start with cts.uris",
			"cts.uris(\"\", null, cts.collectionQuery('Testing'))", newQuery);
	}
}
