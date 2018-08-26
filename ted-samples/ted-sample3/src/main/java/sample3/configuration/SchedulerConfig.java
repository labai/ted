package sample3.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ted.driver.TedDriver;
import ted.scheduler.TedScheduler;

/**
 * @author Augustus
 *         created on 2018.08.25
 */
@Configuration
public class SchedulerConfig {
	private static final Logger logger = LoggerFactory.getLogger(SchedulerConfig.class);


	@Autowired
	private TedDriver tedDriver;

	@Bean(destroyMethod = "shutdown")
	public TedScheduler tedScheduler() {
		TedScheduler scheduler = new TedScheduler(tedDriver);
		scheduler.builder()
				.name(TedConfig.SCHEDULER_TASK_SAMPLE3)
				.scheduleCron("1/3 * * ? * *")
				.runnable(() -> {
					logger.info("Executing scheduler task");
				})
				.register();
		return scheduler;
	}


}
