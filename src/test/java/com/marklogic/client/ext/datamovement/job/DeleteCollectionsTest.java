package com.marklogic.client.ext.datamovement.job;

import com.marklogic.client.datamovement.DeleteListener;
import com.marklogic.client.ext.datamovement.AbstractDataMovementTest;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

public class DeleteCollectionsTest extends AbstractDataMovementTest {

	@Test
	public void test() {
		Properties props = new Properties();
		props.setProperty("collections", COLLECTION);

		DeleteCollectionsJob job = new DeleteCollectionsJob();
		List<String> messages = job.configureJob(props);
		assertTrue("This job doesn't require where* properties to be set", messages.isEmpty());

		assertNotNull(client.newDocumentManager().exists(FIRST_URI));
		assertNotNull(client.newDocumentManager().exists(SECOND_URI));

		job.run(client);

		assertNull(client.newDocumentManager().exists(FIRST_URI));
		assertNull(client.newDocumentManager().exists(SECOND_URI));
	}

	@Test
	public void usingSimpleQueryBatcherJob() {
		SimpleQueryBatcherJob job = new SimpleQueryBatcherJob(new DeleteListener());
		job.setWhereCollections(COLLECTION);

		assertNotNull(client.newDocumentManager().exists(FIRST_URI));
		assertNotNull(client.newDocumentManager().exists(SECOND_URI));

		job.run(client);

		assertNull(client.newDocumentManager().exists(FIRST_URI));
		assertNull(client.newDocumentManager().exists(SECOND_URI));
	}
}
