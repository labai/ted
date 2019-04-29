package ted.driver;


import ted.driver.Ted.PrimeChangeEvent;
import ted.driver.Ted.TedProcessorFactory;
import ted.driver.Ted.TedRetryScheduler;

import java.util.List;

/**
 * @author Augustus
 *         created on 2016.04.29
 *
 */
public interface TedDriverApi {

	interface TedTaskConfig {

		TedRetryScheduler getRetryScheduler();

		//String getChannel(); ...and so on...
	}

	interface TedDriverConfig {

		TedTaskConfig getTaskConfig(String taskName);
	}

	interface TedDriverService {

		void start();

		void shutdown();

	}

	interface TedDriverConfigAware {

		TedDriverConfig getDriverConfig();

	}

	interface TedDriverTaskConfig {

		void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory);

		void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory, TedRetryScheduler retryScheduler);

	}


	interface TedDriverTask {

		Long createTask(String taskName, String data, String key1, String key2);

		Long createTask(String taskName, String data);

		Long createTaskPostponed(String taskName, String data, String key1, String key2, int postponeSec);

		Long createAndExecuteTask(String taskName, String data, String key1, String key2);

		Long createAndStartTask(String taskName, String data, String key1, String key2);

		TedTask getTask(Long taskId);

	}

	interface TedDriverBatch {

		Long createBatch(String batchTaskName, String data, String key1, String key2, List<TedTask> tedTasks);

	}

	interface TedDriverEvent {

		Long createEvent(String taskName, String queueId, String data, String key2);

		Long createEventAndTryExecute(String taskName, String queueId, String data, String key2);

	}

	interface TedDriverNotification {

		Long sendNotification(String taskName, String data);

	}

	interface TedDriverPrime {

		void enablePrime();

		boolean isPrime();

		void setOnBecomePrimeHandler(PrimeChangeEvent onBecomePrime);

		void setOnLostPrimeHandler(PrimeChangeEvent onLostPrime);
	}

}
