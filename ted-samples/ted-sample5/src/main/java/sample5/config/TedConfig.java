package sample5.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import ted.driver.TedResult;
import ted.driver.TedTask;
import ted.spring.annotation.EnableTedTask;
import ted.spring.annotation.TedTaskProcessor;

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

	@TedTaskProcessor(name = "TASK1")
	public TedResult task1(TedTask task) {
		logger.info("start TASK1: {}", task.getData());
		return TedResult.done();
	}

}
