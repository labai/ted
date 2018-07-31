package com.github.labai.ted;


import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author Augustus
 *         created on 2016.09.12
 *
 * 	various TED public interfaces, classes, enums
 */
public class Ted {

	public enum TedDbType {
		ORACLE,
		POSTGRES;
	}

	public enum TedStatus {
		NEW,
		WORK,
		DONE,
		RETRY,
		ERROR,
		SLEEP; // for internal pseudo-tasks
	}

	public static class TedResult {
		private static final TedResult RES_DONE = new TedResult(TedStatus.DONE, null);
		private static final TedResult RES_RETRY = new TedResult(TedStatus.RETRY, null);
		private static final TedResult RES_ERROR = new TedResult(TedStatus.ERROR, null);
		public final TedStatus status;
		public final String message;

		private TedResult(TedStatus status, String message) {
			this.status = status;
			this.message = message;
		}

		public static TedResult error(String msg) {
			return new TedResult(TedStatus.ERROR, msg);
		}
		public static TedResult error() {
			return RES_ERROR;
		}

		public static TedResult done(String msg) {
			return new TedResult(TedStatus.DONE, msg);
		}

		public static TedResult done() {
			return RES_DONE;
		}

		public static TedResult retry(String msg) {
			return new TedResult(TedStatus.RETRY, msg);
		}

		public static TedResult retry() {
			return RES_RETRY;
		}

	}

	/**
	 * TedTask.
	 * 		Tasks info.
	 */
	public static class TedTask {
		private final Long taskId;
		private final String name;
		private final String key1;
		private final String key2;
		private final String data;
		private final Integer retries;
		private final Date createTs;
		private final boolean isNew;
		private final boolean isRetry;
		private final boolean isAfterTimeout;

		public TedTask(Long taskId, String name, String key1, String key2, String data) {
			this(taskId, name, key1, key2, data, 0, new Date(), false);
		}

		public TedTask(Long taskId, String name, String key1, String key2, String data, Integer retries, Date createTs, boolean isAfterTimeout) {
			this.taskId = taskId;
			this.name = name;
			this.key1 = key1;
			this.key2 = key2;
			this.data = data;
			this.retries = retries;
			this.createTs = createTs;
			this.isRetry = retries != null && retries > 0;
			this.isAfterTimeout = isAfterTimeout;
			this.isNew = ! (this.isRetry || this.isAfterTimeout);
		}

		public Long getTaskId() { return taskId; }
		public String getName() { return name; }
		public String getKey1() { return key1; }
		public String getKey2() { return key2; }
		public String getData() { return data; }
		public Integer getRetries() { return retries; }
		public Date getCreateTs() { return createTs; }
		public boolean isRetry() { return isRetry; }
		public boolean isAfterTimeout() { return isAfterTimeout; }
		public boolean isNew() { return isNew; }

		@Override
		public String toString() {
			return "TedTask{" + name + " " + taskId + " key1='" + key1 + '\'' + " key2='" + key2 + '\'' + " retries=" + retries + " createTs=" + createTs + " is=" + (isRetry?"R":"") + (isAfterTimeout ?"T":"") + (isNew?"N":"") + '}';
		}
	}

	/**
	 * TedProcessor.
	 * 		Process task and returns status.
	 */
	public interface TedProcessor {
		TedResult process(TedTask task);
	}

	/**
	 * TedProcessorFactory.
	 * 		Factory, which returns TedProcessor.
	 */
	public interface TedProcessorFactory {
		TedProcessor getProcessor(String taskName);
	}

	/**
	 * TedPackProcessor.
	 * 		Process pack of task and returns their statuses.
	 * 		Returns map (taskId, result) - for all tasks.
	 */
	public interface TedPackProcessor {
		Map<Long, TedResult> process(List<TedTask> tasks);
	}

	/**
	 * TedPackProcessorFactory.
	 * 		Factory, which returns TedPackProcessor.
	 */
	public interface TedPackProcessorFactory {
		TedPackProcessor getPackProcessor(String taskName);
	}

	/**
	 * TedRetryScheduler.
	 * 		Consumer can configure next retry time.
	 */
	public interface TedRetryScheduler {
		Date getNextRetryTime(TedTask task, int retryNumber, Date startTime);
	}


	public interface PrimeEvent {
		void onEvent();
	}
}
