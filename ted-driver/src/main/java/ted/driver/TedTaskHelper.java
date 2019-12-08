package ted.driver;

import ted.driver.sys.TedDriverImpl;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Augustus
 * created on 2019.11.26
 *
 * Helper class to create tasks
 */
public class TedTaskHelper {

	private final TedDriverImpl tedDriverImpl; // for internal, not for in apps


	public TedTaskHelper(TedDriver tedDriver) {
		this.tedDriverImpl = tedDriver.tedDriverImpl;
	}


	public class TaskBuilder {
		private final String name;
		private String key1 = null;
		private String key2 = null;
		private String data = null;
		private int postponeSec = 0;
		private boolean executeImmediately = false;
		private Connection connection = null;

		TaskBuilder(String name) {
			this.name = name;
		}

		public TaskBuilder key1(String key1) {
			this.key1 = key1;
			return this;
		}

		public TaskBuilder key2(String key2) {
			this.key2 = key2;
			return this;
		}

		public TaskBuilder data(String data) {
			this.data = data;
			return this;
		}

		/**
		 * Postpone task for n seconds
		 */
		public TaskBuilder postponeSec(int postponeSec) {
			this.postponeSec = postponeSec;
			return this;
		}

		/**
		 * Will send task directly to execution channel.
		 * Will not wait for execution.
		 * If channel is busy, task may not start immediately.
		 */
		public TaskBuilder executeImmediately(boolean executeImmediately) {
			this.executeImmediately = executeImmediately;
			return this;
		}

		/**
		 * Execute task in provided connection.
		 * Sometime it may be useful to create tasks in
		 * provided db connection (same transaction) as main process:
		 * a) task will not be started until tx finished;
		 * b) task is not be created, if tx rollback.
		 * But consumer is responsible to close connection.
		 */
		public TaskBuilder inConnection(Connection connection) {
			this.connection = connection;
			return this;
		}

		/**
		 * create task
		 */
		public Long create(){
			if (postponeSec > 0) {
				return tedDriverImpl.createTaskPostponed(name, data, key1, key2, postponeSec, connection);
			}
			if (executeImmediately) {
				return tedDriverImpl.createAndExecuteTask(name, data, key1, key2, true, connection);
			}
			return tedDriverImpl.createTask(name, data, key1, key2, null, connection);
		}

		/**
		 * Create task and execute in current thread.
		 * Will wait till task will be finished.
		 */
		public Long createAndWait(){
			return tedDriverImpl.createAndExecuteTask(name, data, key1, key2, false, connection);
		}

	}

	/**
	 * Builder for batch tasks.
	 *
	 * Batch task can have many sub-tasks.
	 * It will wait till all sub-tasks executes.
	 * After all sub-task finished the Batch task will be executed.
	 *
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
			TedTask task = tedDriverImpl.newTedTask(taskName, data, key1, key2);
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
		public Long create(){
			return tedDriverImpl.createBatch(batchName, batchData, batchKey1, batchKey2, tasks);
		}

	}

	/**
	 * Get builder for simple tasks.
	 * Task name is required.
	 */
	public TaskBuilder taskBuilder(String name) {
		return new TaskBuilder(name);
	}

	/**
	 * Get builder for simple tasks.
	 * Task name is required.
	 */
	/* uncomment when need
	public BatchBuilder batchBuilder(String name) {
		return new BatchBuilder(name);
	}
	*/
}
