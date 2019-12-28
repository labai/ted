package ted.driver.task;

import ted.driver.TedTask;
import ted.driver.sys.TedDriverImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author Augustus
 * created on 2019.12.26
 *
 * Helper class to create batch tasks
 * get instance using TedTaskHelper
 *
 * not used yet
 */
public class TedBatchFactory {

	// for internal, not for in apps
	private TedDriverImpl tedDriverImpl = null;
	private Supplier<TedDriverImpl> tedDriverImplAware = null;

	// public for internal
	public TedBatchFactory(TedDriverImpl tedDriverImpl) {
		this.tedDriverImpl = tedDriverImpl;
	}

	// public for internal
	public TedBatchFactory(Supplier<TedDriverImpl> tedDriverImplAware) {
		this.tedDriverImplAware = tedDriverImplAware;
	}

	/**
	 * Get builder for simple tasks.
	 * Task name is required.
	 */
	public BatchBuilder batchBuilder(String batchTaskName) {
		return new BatchBuilder(batchTaskName);
	}


	/**
	 * Builder for batch tasks.
	 * <p>
	 * Batch task can have many sub-tasks.
	 * It will wait till all sub-tasks executes.
	 * After all sub-task finished the Batch task will be executed.
	 */
	public class BatchBuilder {
		private final String batchName;
		private String batchKey1 = null;
		private String batchKey2 = null;
		private String batchData = null;
		private List<TedTask> tasks = new ArrayList<>();

		BatchBuilder(String batchName) {
			this.batchName = batchName;
		}

		public BatchBuilder batchKey1(String key1) {
			this.batchKey1 = key1;
			return this;
		}

		public BatchBuilder batchKey2(String key2) {
			this.batchKey2 = key2;
			return this;
		}

		public BatchBuilder batchData(String data) {
			this.batchData = data;
			return this;
		}

		public BatchBuilder addTask(TedTask task) {
			this.tasks.add(task);
			return this;
		}

		public BatchBuilder addTask(String taskName, String data, String key1, String key2) {
			TedTask task = driver().newTedTask(taskName, data, key1, key2);
			this.tasks.add(task);
			return this;
		}

		public BatchBuilder addTasks(List<TedTask> tasks) {
			this.tasks.addAll(tasks);
			return this;
		}

//		public BatchBuilder clearTasks() {
//			this.tasks.clear();
//			return this;
//		}

		/**
		 * create batch task
		 */
		public Long create() {
			return driver().createBatch(batchName, batchData, batchKey1, batchKey2, tasks);
		}

	}

	private TedDriverImpl driver() {
		if (tedDriverImpl != null)
			return tedDriverImpl;
		synchronized (this) {
			if (tedDriverImplAware == null)
				throw new IllegalStateException("One of tedDriverImpl or tedDriverImplAware must be not null");
			tedDriverImpl = tedDriverImplAware.get();
			if (tedDriverImpl == null)
				throw new IllegalStateException("TedDriverImpl is null");
		}
		return tedDriverImpl;
	}

}
