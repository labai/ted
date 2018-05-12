package com.github.labai.ted.sys;

import com.github.labai.ted.Ted.TedProcessor;
import com.github.labai.ted.Ted.TedResult;
import com.github.labai.ted.Ted.TedTask;
import com.github.labai.ted.sys.Model.TaskRec;
import com.github.labai.ted.sys.TedDriverImpl.TedContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Augustus
 *         created on 2016.09.19
 */
public class I06DriverTest extends TestBase {
	private final static Logger logger = LoggerFactory.getLogger(I06DriverTest.class);

	private TedDriverImpl driver;
	private TedContext context;

	@Override
	protected TedDriverImpl getDriver() { return driver; }

	@Before
	public void init() throws IOException {
		Properties properties = new Properties();
		String propFileName = "ted-I06.properties";
		InputStream inputStream = TestBase.class.getClassLoader().getResourceAsStream(propFileName);
		if (inputStream == null)
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		properties.load(inputStream);

		this.driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID, properties);
		this.context = driver.getContext();
	}



	public static class Test01ProcessorLongOk implements TedProcessor {
		@Override
		public TedResult process(TedTask task)  {
			logger.info(this.getClass().getSimpleName() + " process. sleep for 1000ms");
			try {
				TestUtils.sleepMs(1000);
			} catch (Exception e) {
				//e.printStackTrace();
				return TedResult.error("Interrupted");
			}
			return TedResult.done();
		}
	}

	public static class Test01ProcessorFastOk implements TedProcessor {
		@Override
		public TedResult process(TedTask task)  {
			logger.info(this.getClass().getSimpleName() + " process.");
			TestUtils.sleepMs(10);
			return TedResult.done();
		}
	}

	@Test
	public void test01Shutdown1() throws Exception {
		String taskName = "TEST06-01";
		dao_cleanupAllTasks();

		driver.registerTaskConfig(taskName, TestUtils.forClass(Test01ProcessorLongOk.class), 1, null, Model.CHANNEL_MAIN);
		Long taskId;
		TaskRec taskRec;

		Long taskId1 = driver.createTask(taskName, null, null, null);
		Long taskId2 = driver.createTask(taskName, null, null, null);

		driver.start();
		TestUtils.sleepMs(50);
		// 1 task is processing and other is waiting in queue. last one should be returned to status 'NEW'
		driver.shutdown(50);

		boolean isNew = false;
		boolean isInterrupted = false;

		taskRec = driver.getContext().tedDao.getTask(taskId1);
		if (taskRec.status.equals("NEW"))
			isNew = true;
		else if (taskRec.status.equals("ERROR") && taskRec.msg.equals("Interrupted"))
			isInterrupted = true;

		taskRec = driver.getContext().tedDao.getTask(taskId2);
		if (taskRec.status.equals("NEW"))
			isNew = true;
		else if (taskRec.status.equals("ERROR") && taskRec.msg.equals("Interrupted"))
			isInterrupted = true;

		Assert.assertTrue("One of task is in status NEW?", isNew);
		Assert.assertTrue("One of task is in status ERROR (interrupted)?", isInterrupted);
		//print(taskRec.toString());

		TestUtils.sleepMs(100);
		TestUtils.print("finish");

	}

	@Test
	public void test01Shutdown2() throws Exception {
		String taskName = "TEST06-01";
		dao_cleanupAllTasks();

		driver.registerTaskConfig(taskName, TestUtils.forClass(Test01ProcessorFastOk.class), 1, null, Model.CHANNEL_MAIN);
		Long taskId;
		TaskRec taskRec;

		driver.createTask(taskName, null, null, null);
		driver.createTask(taskName, null, null, null);

		driver.start();
		TestUtils.sleepMs(50);
		driver.shutdown(10); // no working tasks left after 50ms
		TestUtils.print("finish test");

	}

}
