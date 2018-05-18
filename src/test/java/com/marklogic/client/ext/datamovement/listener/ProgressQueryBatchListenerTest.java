package com.marklogic.client.ext.datamovement.listener;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.Forest;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.QueryBatch;
import com.marklogic.client.datamovement.QueryBatcher;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class ProgressQueryBatchListenerTest extends Assert {

	private ProgressQueryBatchListener listener;
	private TestProgressUpdateListener progressUpdateListener = new TestProgressUpdateListener();

	@Test
	public void batchesInOrder() throws Exception {
		listener = new ProgressQueryBatchListener(progressUpdateListener, 4);
		listener.initializeListener(null);
		// Ensure that the total amount of time will be greater than zero for when it's tested below
		Thread.sleep(100);

		listener.processEvent(new FakeQueryBatch(2));
		assertFalse(progressUpdateListener.lastProgressUpdate.isComplete());

		listener.processEvent(new FakeQueryBatch(4));
		assertTrue(progressUpdateListener.lastProgressUpdate.isComplete());

		assertTrue(progressUpdateListener.texts.get(0).startsWith("Progress: 2 of 4; time "));
		assertTrue(progressUpdateListener.texts.get(1).startsWith("Progress: 4 of 4; time "));
		assertEquals(2, progressUpdateListener.texts.size());

		// Verify the values on the ProgressUpdate are correct
		assertEquals(4, progressUpdateListener.lastProgressUpdate.getResultsCount());
		assertEquals(4, progressUpdateListener.lastProgressUpdate.getTotalResults());
		assertTrue(progressUpdateListener.lastProgressUpdate.getStartTime() > 0);
		assertTrue(progressUpdateListener.lastProgressUpdate.getTimeSoFarInSeconds() > 0);
	}

	@Test
	public void batchesOutOfOrder() {
		listener = new ProgressQueryBatchListener(progressUpdateListener, 4);
		listener.initializeListener(null);
		listener.processEvent(new FakeQueryBatch(4));
		listener.processEvent(new FakeQueryBatch(2));

		assertTrue(progressUpdateListener.texts.get(0).startsWith("Progress: 4 of 4"));
		assertEquals("The second batch is ignored because its jobResultsSoFar value is less than what the listener has seen so far",
			1, progressUpdateListener.texts.size());
	}

	@Test
	public void noTotalResults() {
		listener = new ProgressQueryBatchListener(progressUpdateListener);
		listener.initializeListener(null);
		listener.processEvent(new FakeQueryBatch(2));
		listener.processEvent(new FakeQueryBatch(4));

		assertTrue(progressUpdateListener.texts.get(0).startsWith("Progress: 2 results so far; time "));
		assertTrue(progressUpdateListener.texts.get(1).startsWith("Progress: 4 results so far; time "));
		assertEquals(2, progressUpdateListener.texts.size());
	}
}

class TestProgressUpdateListener implements ProgressQueryBatchListener.ProgressUpdateListener {
	public List<String> texts = new ArrayList<>();
	public ProgressQueryBatchListener.ProgressUpdate lastProgressUpdate;
	@Override
	public void onUpdatedProgress(ProgressQueryBatchListener.ProgressUpdate progressUpdate) {
		lastProgressUpdate = progressUpdate;
		texts.add(progressUpdate.getProgressAsString());
	}
}

class FakeQueryBatch implements QueryBatch {

	private long jobResultsSoFar;

	public FakeQueryBatch(long jobResultsSoFar) {
		this.jobResultsSoFar = jobResultsSoFar;
	}

	@Override
	public long getServerTimestamp() {
		return 0;
	}

	@Override
	public String[] getItems() {
		return new String[0];
	}

	@Override
	public Calendar getTimestamp() {
		return null;
	}

	@Override
	public QueryBatcher getBatcher() {
		return null;
	}

	@Override
	public DatabaseClient getClient() {
		return null;
	}

	@Override
	public long getJobBatchNumber() {
		return 0;
	}

	@Override
	public long getJobResultsSoFar() {
		return jobResultsSoFar;
	}

	@Override
	public long getForestBatchNumber() {
		return 0;
	}

	@Override
	public long getForestResultsSoFar() {
		return 0;
	}

	@Override
	public Forest getForest() {
		return null;
	}

	@Override
	public JobTicket getJobTicket() {
		return null;
	}
}