package labai.ted.sys;

import labai.ted.Ted.TedProcessor;
import labai.ted.TedResult;
import labai.ted.TedTask;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Augustus
 *         created on 2016.09.20
 */

@Ignore
public class I05PerfomTest extends TestBase {
	private final static Logger logger = LoggerFactory.getLogger(I05PerfomTest.class);

	private TedDriverImpl driver;

	@Override
	protected TedDriverImpl getDriver() { return driver; }

	@Before
	public void init() throws IOException {
		Properties properties = new Properties();
		String propFileName = "ted-I05.properties";
		InputStream inputStream = TestBase.class.getClassLoader().getResourceAsStream(propFileName);
		if (inputStream == null)
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		properties.load(inputStream);

		driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID, properties);
	}


	public static class Test05ProcessorOk implements TedProcessor {
		@Override
		public TedResult process(TedTask task)  {
			logger.info(this.getClass().getSimpleName() + " process");
			//sleepMs(2000);
			return TedResult.done();
		}
	}


	@Test
	public void test01FullQueue() throws Exception {
		/* fill:
		(oracle)
		insert into tedtask (taskId, system, name, channel, bno, status, createTs, nextTs)
		select SEQ_TEDTASK_ID.nextval, 'ted.test', 'TEST05-01', 'TEST5', null, 'NEW', systimestamp, systimestamp
		from dual connect by level <= 1000;
		commit;
		(postgre)
		insert into tedtask (taskId, system, name, channel, bno, status, createTs, nextTs)
		select nextval('SEQ_TEDTASK_ID'), 'ted.test', 'TEST05-01', 'TEST5', null, 'NEW', now(), now()
		from generate_series(1,100) s;
		*/
		// dao_cleanupAllTasks();
		String taskName = "TEST05-01";
		driver.registerTaskConfig(taskName, TestTedProcessors.forClass(Test05ProcessorOk.class));
		try {
			for (int i = 0; i < 30; i++) {
				getContext().taskManager.processChannelTasks();
				TestUtils.sleepMs(600);
				if (getContext().tedDao.getWaitChannels().isEmpty()) {
					TestUtils.print("No more tasks, finish");
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		TestUtils.print("Exit");
	}
}
