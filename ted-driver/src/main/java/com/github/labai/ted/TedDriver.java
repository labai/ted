package com.github.labai.ted;

import com.github.labai.ted.Ted.TedDbType;
import com.github.labai.ted.Ted.TedProcessorFactory;
import com.github.labai.ted.Ted.TedRetryScheduler;
import com.github.labai.ted.Ted.TedTask;
import com.github.labai.ted.sys.TedDriverImpl;

import javax.sql.DataSource;
import java.util.List;
import java.util.Properties;

/**
 * @author Augustus
 *         created on 2016.09.12
 *
 *  TedDriver with api
 *
 */
public class TedDriver {
	private TedDriverImpl tedDriverImpl;

	/**
	 *  dataSource - provides oracle db (with tedtask table) connection dataSource;
	 *  properties - ted configuration
	 */
	public TedDriver(TedDbType dbType, DataSource dataSource, Properties properties) {
		tedDriverImpl = new TedDriverImpl(dbType, dataSource, properties);
	}

	/**
	 * start TED task manager
	 */
	public void start() {
		tedDriverImpl.start();
	}

	/**
	 * shutdown TED
	 */
	public void shutdown() {
		tedDriverImpl.shutdown(20*1000);
	}

	/**
	 * create task (to perform)
	 *
	 */
	public Long createTask(String taskName, String data, String key1, String key2) {
		return tedDriverImpl.createTask(taskName, data, key1, key2, null);
	}

	/** simple version */
	public Long createTask(String taskName, String data) {
		return tedDriverImpl.createTask(taskName, data, null, null, null);
	}

	/** create task for future execution (postponed) */
	public Long createTaskPostponed(String taskName, String data, String key1, String key2, int postponeSec) {
		return tedDriverImpl.createTaskPostponed(taskName, data, key1, key2, postponeSec);
	}

	/** create task and immediately execute it (will wait until execution finish) */
	public Long createAndExecuteTask(String taskName, String data, String key1, String key2) {
		return tedDriverImpl.createAndExecuteTask(taskName, data, key1, key2, false);
	}

	/** create task and start to process it in channel (will NOT wait until execution finish) */
	public Long createAndStartTask(String taskName, String data, String key1, String key2) {
		return tedDriverImpl.createAndExecuteTask(taskName, data, key1, key2, true);
	}


	/** incubating.
	 * create all tasks and 1 batch tasks (see *.properties configurations).
	 * to create TedTask object use function newTedTask
	 */
	// create tasks by list and batch task for them. return batch taskId
	public Long createBatch(List<TedTask> tedTasks) {
		return tedDriverImpl.createBatch(tedTasks);
	}

	/** incubating.
	 * create TedTask for createBatch (with required params only)
	 */
	public static TedTask newTedTask(String taskName, String data, String key1, String key2) {
		return new TedTask(null, taskName, key1, key2, data, 0, null);
	}

	/**
	 * register task (configuration)
	 *
	 */
	public void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory) {
		tedDriverImpl.registerTaskConfig(taskName, tedProcessorFactory);
	}

	/**
	 * register task (configuration).
	 * use pack processor
	 */
// (disabled yet - is it useful?)
//	public void registerTaskConfig(String taskName, TedPackProcessorFactory tedPackProcessorFactory) {
//		tedDriverImpl.registerTaskConfig(taskName, tedPackProcessorFactory);
//	}

	/**
	 * register task (configuration) with own retryScheduler
	 *
	 */
	public void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory, TedRetryScheduler retryScheduler) {
		tedDriverImpl.registerTaskConfig(taskName, tedProcessorFactory, null, retryScheduler, null);
	}

	/**
	 * register channel
	 *
	 */
	public void registerChannel(String queueName, int workerCount, int taskBufferSize) {
		tedDriverImpl.registerChannel(queueName, workerCount, taskBufferSize);
	}

}
