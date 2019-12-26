package ted.driver.task;


import ted.driver.sys.TedDriverImpl;

import java.sql.Connection;
import java.util.function.Supplier;

/**
 * @author Augustus
 * created on 2019.12.26
 *
 * Helper class to create tasks
 * get instance using TedTaskHelper
 *
 */
public class TedTaskFactory {

	// for internal, not for in apps
	private TedDriverImpl tedDriverImpl = null;
	private Supplier<TedDriverImpl> tedDriverImplAware = null;


	// public for internal
	public TedTaskFactory(TedDriverImpl tedDriverImpl) {
		this.tedDriverImpl = tedDriverImpl;
	}

	// public for internal
	public TedTaskFactory(Supplier<TedDriverImpl> tedDriverImplAware) {
		this.tedDriverImplAware = tedDriverImplAware;
	}

	/**
	 * simplest way to create task
	 */
	public Long createTask(String taskName, String data) {
		return driver().createTask(taskName, data, null, null, null, null);
	}


	/**
	 * Get builder for task.
	 *
	 * Task name is required.
	 */
	public TaskBuilder taskBuilder(String taskName) {
		return new TaskBuilder(taskName);
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
				return driver().createTaskPostponed(name, data, key1, key2, postponeSec, connection);
			}
			if (executeImmediately) {
				return driver().createAndExecuteTask(name, data, key1, key2, true, connection);
			}
			return driver().createTask(name, data, key1, key2, null, connection);
		}

		/**
		 * Create task and execute in current thread.
		 * Will wait till task will be finished.
		 */
		public Long createAndWait(){
			return driver().createAndExecuteTask(name, data, key1, key2, false, connection);
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
