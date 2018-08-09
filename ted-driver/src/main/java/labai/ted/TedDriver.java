package labai.ted;

import labai.ted.Ted.PrimeChangeEvent;
import labai.ted.Ted.TedDbType;
import labai.ted.Ted.TedProcessorFactory;
import labai.ted.Ted.TedRetryScheduler;
import labai.ted.sys.TedDriverImpl;

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

	/** create task (to perform) */
	public Long createTask(String taskName, String data, String key1, String key2) {
		return tedDriverImpl.createTask(taskName, data, key1, key2, null);
	}

	/** create task - simple version */
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

	/** create tasks by list and batch task for them. return batch taskId */
	public Long createBatch(String batchTaskName, String data, String key1, String key2, List<TedTask> tedTasks) {
		return tedDriverImpl.createBatch(batchTaskName, data, key1, key2, tedTasks);
	}

	/** create event in queue */
	public Long createEvent(String taskName, String discriminator, String data, String key2) {
		return tedDriverImpl.createEvent(taskName, discriminator, data, key2);
	}

	/** create event in queue. If possible, try to execute */
	public Long createAndTryExecuteEvent(String taskName, String discriminator, String data, String key2) {
		return tedDriverImpl.createAndTryExecuteEvent(taskName, discriminator, data, key2);
	}

	/**
	 * create TedTask for createBatch (with required params only)
	 */
	public static TedTask newTedTask(String taskName, String data, String key1, String key2) {
		return new TedTask(null, taskName, key1, key2, data);
	}

	/**
	 * register task (configuration)
	 */
	public void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory) {
		tedDriverImpl.registerTaskConfig(taskName, tedProcessorFactory);
	}

	/**
	 * register task (configuration) with own retryScheduler
	 */
	public void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory, TedRetryScheduler retryScheduler) {
		tedDriverImpl.registerTaskConfig(taskName, tedProcessorFactory, null, retryScheduler, null);
	}

	//
	// prime instance
	//

	public void enablePrime() {
		tedDriverImpl.prime().enable();
	}

	public boolean isPrime() {
		return tedDriverImpl.prime().isPrime();
	}

	public void setOnBecomePrimeHandler(PrimeChangeEvent onBecomePrime) {
		tedDriverImpl.prime().setOnBecomePrime(onBecomePrime);
	}

	public void setOnLostPrimeHandler(PrimeChangeEvent onLostPrime) {
		tedDriverImpl.prime().setOnLostPrime(onLostPrime);
	}
}
