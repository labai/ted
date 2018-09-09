package ted.driver.sys;

import ted.driver.Ted.TedProcessor;
import ted.driver.TedResult;
import ted.driver.TedTask;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.TedDriverImpl.TedContext;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

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
		Properties properties = TestUtils.readPropertiesFile("ted-I06.properties");
		this.driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID, properties);
		this.context = driver.getContext();
	}



	public static class Test06ProcessorLongOk implements TedProcessor {
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

	public static class Test06ProcessorFastOk implements TedProcessor {
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

		driver.registerTaskConfig(taskName, TestTedProcessors.forClass(Test06ProcessorLongOk.class), 1, null, Model.CHANNEL_MAIN);
		Long taskId;
		TaskRec taskRec;

		Long taskId1 = driver.createTask(taskName, null, null, null);
		Long taskId2 = driver.createTask(taskName, null, null, null);

		driver.start();
		TestUtils.sleepMs(200);
		// 1 task is processing and other is waiting in queue. last one should be returned to status 'NEW'
		TestUtils.print("Start to shutdown");
		driver.shutdown(100);
		TestUtils.print("Shutdown finished");

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

		assertTrue("One of task is in status NEW?", isNew);
		assertTrue("One of task is in status ERROR (interrupted)?", isInterrupted);
		//print(taskRec.toString());

		TestUtils.sleepMs(100);
		TestUtils.print("finish");

	}

	@Test
	public void test01Shutdown2() throws Exception {
		String taskName = "TEST06-01";
		dao_cleanupAllTasks();

		driver.registerTaskConfig(taskName, TestTedProcessors.forClass(Test06ProcessorFastOk.class), 1, null, Model.CHANNEL_MAIN);
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
