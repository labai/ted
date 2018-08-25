package sample3.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import sample3.configuration.TedConfig;
import ted.driver.TedResult;
import ted.driver.TedTask;

import java.util.Random;

/**
 * @author Augustus
 *         created on 2018.08.25
 */
@Component
public class ProcessLineTask extends AbstractTedProcessor {
	private static final Logger logger = LoggerFactory.getLogger(AbstractTedProcessor.class);

	public ProcessLineTask() {
		super(TedConfig.SAMPLE_TASK_NAME);
	}

	private Random random = new Random();

	@Override
	public TedResult process(TedTask task) {
		int sleepMs = 200 + random.nextInt(700);
		logger.info("do something smart with line: '{}' for {}ms", task.getData(), sleepMs);
		sleep(sleepMs);
		return TedResult.done();
	}

	private static void sleep(long milis) {
		try {
			Thread.sleep(milis);
		} catch (InterruptedException e2) {
		}
	}

}
