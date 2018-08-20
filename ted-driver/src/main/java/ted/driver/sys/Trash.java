package ted.driver.sys;

import ted.driver.TedResult;
import ted.driver.TedTask;

import java.util.List;
import java.util.Map;

// for something still disabled
class Trash {

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
	 * register task (configuration).
	 * use pack processor
	 */
//	public void registerTaskConfig(String taskName, TedPackProcessorFactory tedPackProcessorFactory) {
//		tedDriverImpl.registerTaskConfig(taskName, tedPackProcessorFactory);
//	}

	/** create task for key1, ensure only 1 active task with same key1 value. Returns null if already exists. To be 100% sure, unique index should be created (ix_tedtask_key1_uniq) */
//	public Long createTaskUniqueKey1(String taskName, String data, String key1, String key2) {
//		return tedDriverImpl.createTaskUniqueKey1(taskName, data, key1, key2);
//	}

	/** register channel */
//	public void registerChannel(String queueName, int workerCount, int taskBufferSize) {
//		tedDriverImpl.registerChannel(queueName, workerCount, taskBufferSize);
//	}


}
