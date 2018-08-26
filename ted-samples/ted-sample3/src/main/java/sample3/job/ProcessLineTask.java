package sample3.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import sample3.configuration.TedConfig;
import sample3.job.ProcessLineTask.TaskParam;
import ted.driver.TedResult;
import ted.driver.TedTask;

import java.util.Random;

/**
 * @author Augustus
 *         created on 2018.08.25
 */
@Component
public class ProcessLineTask extends AbstractTedProcessor<TaskParam> {
	private static final Logger logger = LoggerFactory.getLogger(AbstractTedProcessor.class);

	public ProcessLineTask() {
		super(TedConfig.TASK_PROCESS_LINE);
	}

	private Random random = new Random();

	public static class TaskParam {
		public int lineNumber;
		public String line;
	}

	@Override
	protected final TedResult processTask(TedTask task, TaskParam data) {
		int sleepMs = 200 + random.nextInt(700);
		logger.info("do something smart with line {}: '{}' for {}ms", data.lineNumber, data.line, sleepMs);
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
