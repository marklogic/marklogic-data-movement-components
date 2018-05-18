package com.marklogic.client.ext.datamovement.listener;

import com.marklogic.client.datamovement.QueryBatch;
import com.marklogic.client.datamovement.QueryBatchListener;
import com.marklogic.client.datamovement.QueryBatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * QueryBatchListener for receiving progress updates as QueryBatch instances are processed. Progress updates are
 * communicated via instances of the nested static ProgressUpdate class, which are given to an implementation of the
 * nested static ProgressUpdateListener callback interface.
 */
public class ProgressListener implements QueryBatchListener {

	private static Logger logger = LoggerFactory.getLogger(ProgressListener.class);

	private List<Consumer<ProgressUpdate>> consumers = new ArrayList<>();
	private AtomicLong resultsSoFar = new AtomicLong(0);
	private long startTime;
	private long totalResults;

	public ProgressListener() {
	}

	/**
	 * Use this constructor for when the total number of results isn't known ahead of time.
	 *
	 * @param consumers
	 */
	public ProgressListener(Consumer<ProgressUpdate>... consumers) {
		this(0, consumers);
	}

	/**
	 * Use this constructor for when the total number of results is known ahead of time. The ProgressUpdate objects that
	 * are emitted will have more interesting information as a result.
	 *
	 * @param consumers
	 * @param totalResults
	 */
	public ProgressListener(long totalResults, Consumer<ProgressUpdate>... consumers) {
		this.totalResults = totalResults;
		for (Consumer<ProgressUpdate> consumer : consumers) {
			this.consumers.add(consumer);
		}
	}

	public ProgressListener withTotalResults(long totalResults) {
		this.totalResults = totalResults;
		return this;
	}

	public ProgressListener onProgressUpdate(Consumer<ProgressUpdate> consumer) {
		this.consumers.add(consumer);
		return this;
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
	 * Batches arrive in random order, so a ProgressUpdate is created and sent to each Consumer only if the
	 * value of "getJobResultsSoFar" on the QueryBatch exceeds the number of results seen so far.
	 * <p>
	 * For example, if there are 2 batches, and batch 2 is processed first by this listener followed by batch 1, a
	 * ProgressUpdate is only created when batch 2 is processed.
	 *
	 * @param batch
	 */
	@Override
	public void processEvent(QueryBatch batch) {
		// resultsSoFar is an AtomicLong so it can be safely updated across many threads
		final long jobResultsSoFar = batch.getJobResultsSoFar();
		final long newResultsSoFar = this.resultsSoFar.updateAndGet(operand ->
			jobResultsSoFar > operand ? jobResultsSoFar : operand
		);
		boolean resultsSoFarWasUpdated = jobResultsSoFar == newResultsSoFar;

		if (resultsSoFarWasUpdated && consumers != null) {
			double timeSoFar = ((double) System.currentTimeMillis() - startTime) / 1000;

			/**
			 * The initial totalResults may have been incorrectly set to a value lower than jobResultsSoFar; if this
			 * occurs, use jobResultsSoFar as the value passed to the ProgressUpdate object. totalResults is not
			 * updated though in case there's a need to know what the initial value was.
			 */
			long totalForThisUpdate = jobResultsSoFar > this.totalResults && this.totalResults > 0 ? jobResultsSoFar : this.totalResults;

			ProgressUpdate progressUpdate = new SimpleProgressUpdate(batch, startTime, totalForThisUpdate, timeSoFar);

			for (Consumer<ProgressUpdate> consumer : consumers) {
				invokeConsumer(consumer, progressUpdate);
			}
		}
	}

	/**
	 * Protected so that a subclass can override how a consumer is invoked, particularly how an exception is handled.
	 *
	 * @param consumer
	 * @param progressUpdate
	 */
	protected void invokeConsumer(Consumer<ProgressUpdate> consumer, ProgressUpdate progressUpdate) {
		try {
			consumer.accept(progressUpdate);
		} catch (Throwable t) {
			logger.error("Exception thrown by a Consumer<ProgressUpdate> consumer: " + consumer + "; progressUpdate: " + progressUpdate, t);
		}
	}

	/**
	 * Captures data of interest for a progress update.
	 */
	public interface ProgressUpdate {

		String getProgressAsString();

		boolean isComplete();

		QueryBatch getQueryBatch();

		long getStartTime();

		long getTotalResults();

		double getTimeSoFarInSeconds();
	}

	/**
	 * Simple implementation of ProgressUpdate; only real thing of interest in here is how it generates the progress
	 * as a string for display purposes.
	 */
	public static class SimpleProgressUpdate implements ProgressUpdate {

		private QueryBatch queryBatch;
		private long startTime;
		private long totalResults;
		private double timeSoFarInSeconds;

		public SimpleProgressUpdate(QueryBatch queryBatch, long startTime, long totalResults, double timeSoFarInSeconds) {
			this.queryBatch = queryBatch;
			this.startTime = startTime;
			this.timeSoFarInSeconds = timeSoFarInSeconds;
			this.totalResults = totalResults;
		}

		@Override
		public String getProgressAsString() {
			if (totalResults > 0) {
				String text = String.format("Progress: %d of %d; time %fs", queryBatch.getJobResultsSoFar(), totalResults, timeSoFarInSeconds);
				if (timeSoFarInSeconds > 0) {
					double rate = queryBatch.getJobResultsSoFar() / timeSoFarInSeconds;
					BigDecimal bd = new BigDecimal(rate);
					rate = bd.round(new MathContext(5)).doubleValue();
					return text + "; " + rate + " records/s";
				}
				return text;
			}

			return String.format("Progress: %d results so far; time %fs", queryBatch.getJobResultsSoFar(), timeSoFarInSeconds);
		}

		@Override
		public boolean isComplete() {
			return totalResults > 0 ? queryBatch.getJobResultsSoFar() >= totalResults : false;
		}

		@Override
		public QueryBatch getQueryBatch() {
			return queryBatch;
		}

		@Override
		public long getStartTime() {
			return startTime;
		}

		@Override
		public long getTotalResults() {
			return totalResults;
		}

		@Override
		public double getTimeSoFarInSeconds() {
			return timeSoFarInSeconds;
		}
	}
}
