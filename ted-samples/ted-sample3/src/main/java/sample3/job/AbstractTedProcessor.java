package sample3.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import ted.driver.Ted.TedProcessor;
import ted.driver.TedDriver;
import ted.driver.TedResult;
import ted.driver.TedTask;

import javax.annotation.PostConstruct;

/**
 * @author Augustus
 *         created on 2018.08.25
 *
 * sample AbstractTedProcessor.
 *
 * This TedProcessor is single component, thus do not hold state here.
 *
*/
abstract class AbstractTedProcessor implements TedProcessor {
	private static final Logger logger = LoggerFactory.getLogger(AbstractTedProcessor.class);

	@Autowired
	private TedDriver tedDriver;

	private final String taskName;

	/**
	 * can handle onAfterTimeout.
	 * It will be called before main task processing.
	 * If returns null - then will continue and processTask will be called,
	 * otherwise, if some TedResult will be returned, it will be set to task in db.
	 */
	protected TedResult onAfterTimeout(TedTask task) {
		return null;
	};

	AbstractTedProcessor(String taskName) {
		this.taskName = taskName;
	}

	@PostConstruct
	public final void registerTask() {
		tedDriver.registerTaskConfig(taskName, taskName -> this::processInternal);
	}

	private TedResult processInternal(TedTask task) {
		if (task.isAfterTimeout()) {
			TedResult res = onAfterTimeout(task);
			if (res != null)
				return res;
		}
		return process(task);
	}

}
