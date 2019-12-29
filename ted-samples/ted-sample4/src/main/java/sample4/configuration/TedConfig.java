package sample4.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import ted.driver.TedResult;
import ted.driver.TedTask;
import ted.spring.annotation.TedTaskProcessor;
import ted.spring.annotation.EnableTedTask;
import ted.spring.conf.TedRetryException;

/**
 * @author Augustus
 *         created on 2019.12.04
 *
 *  just add
 *  	EnableTedTask annotation
 */
@EnableTedTask
@Configuration
public class TedConfig {
	private static final Logger logger = LoggerFactory.getLogger(TedConfig.class);

	public static class Sample4RetryException extends TedRetryException {
		public Sample4RetryException(String message) {
			super(message);
		}
	}

	// task may return TedResult
	@TedTaskProcessor(name = "TASK1")
	public TedResult task1(TedTask task) {
		logger.info("start TASK1: {}", task.getData());
		return TedResult.done();
	}

	// task may throw exception. If it is TedRetryException then task will be retried
	@TedTaskProcessor(name = "TASK2")
	public String task2(TedTask task) {
		logger.info("start TASK2");
		throw new Sample4RetryException("retry error");
	}
}
