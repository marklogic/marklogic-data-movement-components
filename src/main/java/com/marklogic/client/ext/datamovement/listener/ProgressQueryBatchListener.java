package com.marklogic.client.ext.datamovement.listener;

import com.marklogic.client.datamovement.QueryBatch;
import com.marklogic.client.datamovement.QueryBatchListener;
import com.marklogic.client.datamovement.QueryBatcher;

import java.math.BigDecimal;
import java.math.MathContext;

/**
 * QueryBatchListener for receiving progress updates as QueryBatch instances are processed. Progress updates are
 * communicated via instances of the nested static ProgressUpdate class, which are given to an implementation of the
 * nested static ProgressUpdateListener callback interface.
 */
public class ProgressQueryBatchListener implements QueryBatchListener {

	private ProgressUpdateListener progressListener;

	private long startTime;
	private long totalResults;
	private long resultsCount;

	/**
	 * Use this constructor for when the total number of results isn't known ahead of time.
	 *
	 * @param progressListener
	 */
	public ProgressQueryBatchListener(ProgressUpdateListener progressListener) {
		this(progressListener, 0);
	}

	/**
	 * Use this constructor for when the total number of results is known ahead of time. The ProgressUpdate objects that
	 * are emitted will have more interesting information as a result.
	 *
	 * @param progressListener
	 * @param totalResults
	 */
	public ProgressQueryBatchListener(ProgressUpdateListener progressListener, long totalResults) {
		this.progressListener = progressListener;
		this.totalResults = totalResults;
	}

	/**
	 * Initializes the start time so that each ProgressUpdate knows how long it occurred after the job was started.
	 *
	 * @param queryBatcher
	 */
	@Override
	public void initializeListener(QueryBatcher queryBatcher) {
		startTime = System.currentTimeMillis();
	}

	/**
	 * Batches arrive in random order, so a ProgressUpdate is created and sent to the ProgressUpdateListener only if the
	 * value of "getJobResultsSoFar" on the QueryBatch exceeds the number of results seen so far.
	 * <p>
	 * For example, if there are 2 batches, and batch 2 is processed first by this listener followed by batch 1, a
	 * ProgressUpdate is only created when batch 2 is processed.
	 *
	 * @param batch
	 */
	@Override
	public void processEvent(QueryBatch batch) {
		boolean resultsCountUpdated = updateResultsCountIfNecessary(batch);
		if (resultsCountUpdated && progressListener != null) {
			double timeSoFar = ((double) System.currentTimeMillis() - startTime) / 1000;
			progressListener.onUpdatedProgress(new ProgressUpdate(startTime, totalResults, batch.getJobResultsSoFar(), timeSoFar));
		}
	}

	/**
	 * This is protected so that a subclass can make this synchronized if desired. Without this being synchronized, there
	 * is a slight chance of a race condition between two threads where the results count could be set to an incorrect
	 * value. This is deemed tolerable though since the intent of this listener is only to report progress.
	 *
	 * @param batch
	 * @return
	 */
	protected boolean updateResultsCountIfNecessary(QueryBatch batch) {
		long jobResultsSoFar = batch.getJobResultsSoFar();
		if (jobResultsSoFar > resultsCount) {
			setResultsCount(jobResultsSoFar);
			return true;
		}
		return false;
	}

	public void setResultsCount(long resultsCount) {
		this.resultsCount = resultsCount;
	}

	/**
	 * Callback interface for when ProgressQueryBatchListener creates an instance of ProgressUpdate.
	 */
	public interface ProgressUpdateListener {
		void onUpdatedProgress(ProgressUpdate progressUpdate);
	}

	public static class ProgressUpdate {

		private long startTime;
		private long totalResults;
		private long resultsCount;
		private double timeSoFarInSeconds;

		public ProgressUpdate(long startTime, long totalResults, long resultsCount, double timeSoFarInSeconds) {
			this.startTime = startTime;
			this.totalResults = totalResults;
			this.resultsCount = resultsCount;
			this.timeSoFarInSeconds = timeSoFarInSeconds;

			// If totalResults is set, ensure that resultsCount never exceeds it
			if (totalResults > 0 && resultsCount > totalResults) {
				this.resultsCount = totalResults;
			}
		}

		public String getProgressAsString() {
			if (totalResults > 0) {
				String text = String.format("Progress: %d of %d; time %fs", resultsCount, totalResults, timeSoFarInSeconds);
				if (timeSoFarInSeconds > 0) {
					double rate = resultsCount / timeSoFarInSeconds;
					BigDecimal bd = new BigDecimal(rate);
					rate = bd.round(new MathContext(5)).doubleValue();
					return text + "; " + rate + " records/s";
				}
				return text;
			}

			return String.format("Progress: %d results so far; time %fs", resultsCount, timeSoFarInSeconds);
		}

		public boolean isComplete() {
			return totalResults > 0 ? resultsCount >= totalResults : false;
		}

		public long getStartTime() {
			return startTime;
		}

		public long getTotalResults() {
			return totalResults;
		}

		public long getResultsCount() {
			return resultsCount;
		}

		public double getTimeSoFarInSeconds() {
			return timeSoFarInSeconds;
		}
	}
}
