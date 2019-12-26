package ted.spring.conf;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import ted.driver.TedDriver;
import ted.driver.TedTaskHelper;

/**
 * @author Augustus
 *         created on 2019.12.04
 *
 * Can be used for taskCreation
 *
 */
@Configuration
public class TedTaskService {

	@Autowired
	private ApplicationContext applicationContext;

	private TedDriver tedDriver = null;
	private TedTaskHelper tedTaskHelper = null;

	public Long createTask(String name, String data) {
		if (tedDriver == null) {
			synchronized (this) {
				if (tedDriver == null) {
					tedDriver = applicationContext.getBean(TedDriver.class);
					tedTaskHelper = new TedTaskHelper(tedDriver);
				}
			}
		}
		return tedTaskHelper.getTaskFactory().taskBuilder(name)
				.data(data)
				.create();
	}
}
