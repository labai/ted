package ted.driver;


import java.util.Date;

/**
 * @author Augustus
 *         created on 2016.09.12
 *
 * 	various TED public interfaces, classes, enums
 */
public final class Ted {

	public enum TedDbType {
		ORACLE,
		POSTGRES,
		MYSQL,
		HSQLDB; // for tests
	}

	public enum TedStatus {
		NEW,	// new task
		WORK,	// task is taken to process
		DONE,	// task successfully processed
		RETRY,	// temporal obstacles occurred while processing task, will be retried later
		ERROR,	// task finished with error
		SLEEP; 	// task is created, but will not be taken to processing. For eventsQueue it will be processed after previous tasks finished
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
	 * TedRetryScheduler.
	 * 		Consumer can configure next retry time.
	 */
	public interface TedRetryScheduler {
		Date getNextRetryTime(TedTask task, int retryNumber, Date startTime);
	}

	/**
	 * PrimeChangeEvent.
	 * 		For onBecomePrime and onLostPrime.
	 */
	public interface PrimeChangeEvent {
		void onEvent();
	}
}
