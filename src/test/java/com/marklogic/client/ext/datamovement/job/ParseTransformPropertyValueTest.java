package com.marklogic.client.ext.datamovement.job;

import com.marklogic.client.document.ServerTransform;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class ParseTransformPropertyValueTest extends Assert {

	@Test
	public void twoParams() {
		ParseTransformPropertyValueJob job = new ParseTransformPropertyValueJob();
		Properties props = new Properties();
		props.setProperty("transform", "myTransform,param1,value1,param2,value2");
		job.configureJob(props);

		assertEquals("myTransform", job.transform.getName());
		assertEquals("value1", job.transform.get("param1").get(0));
		assertEquals("value2", job.transform.get("param2").get(0));
	}

	@Test
	public void noParams() {
		ParseTransformPropertyValueJob job = new ParseTransformPropertyValueJob();
		Properties props = new Properties();
		props.setProperty("transform", "myTransform");
		job.configureJob(props);

		assertEquals("myTransform", job.transform.getName());
		assertTrue(job.transform.isEmpty());
	}
}

class ParseTransformPropertyValueJob extends AbstractQueryBatcherJob {

	public ServerTransform transform;

	public ParseTransformPropertyValueJob() {
		addTransformJobProperty((value, transform) -> this.transform = transform);
	}

	@Override
	protected String getJobDescription() {
		return null;
	}
}
