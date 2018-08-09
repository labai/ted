package labai.ted.sys;

import labai.ted.Ted.TedProcessor;
import labai.ted.Ted.TedStatus;
import labai.ted.TedResult;
import labai.ted.TedTask;
import labai.ted.sys.JdbcSelectTed.SqlParam;
import labai.ted.sys.Model.TaskRec;
import labai.ted.sys.TedDriverImpl.TedContext;
import labai.ted.sys.TestTedProcessors.TestProcessorException;
import labai.ted.sys.TestTedProcessors.TestProcessorOk;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static labai.ted.sys.TestConfig.SYSTEM_ID;
import static labai.ted.sys.TestTedProcessors.forClass;
import static org.junit.Assert.*;

/**
 * @author Augustus
 *         created on 2016.09.19
 */
public class I01SimpleTest extends TestBase {
	private final static Logger logger = LoggerFactory.getLogger(I01SimpleTest.class);

	private TedDriverImpl driver;
	private TedContext context;

	@Override
	protected TedDriverImpl getDriver() { return driver; }

	@Before
	public void init() throws IOException {
		Properties properties = TestUtils.readPropertiesFile("ted-I01.properties");
		driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), SYSTEM_ID, properties);
		this.context = driver.getContext();
	}


	@Ignore // data not limited
	@Test
	public void test01ClobVarchar() throws Exception {
		String taskName = "TEST01-01";
		driver.registerTaskConfig(taskName, forClass(TestProcessorOk.class));

		// long string
		String param = "";
		for (int i = 0; i < 1000; i++) {
			param += i + " ---------------------------------------------------------------------------------------------------- " + i + "\n";
		}
		Long taskId = null;
		try {
			taskId = driver.createTask(taskName, ("test long param\n" + param), null, null);
			fail("Expected string too long exception");
		} catch (Exception e) {
		}

//		TaskRec taskRec = driver.getContext().tedDao.getTask(taskId);
//		assertEquals("NEW", taskRec.status);
//		assertEquals("test long param\n" + param, new String(taskRec.data));
//		driver.getContext().tedDao.setStatus(taskId, TedStatus.DONE, "test ok");
//		taskRec = driver.getContext().tedDao.getTask(taskId);
//		assertEquals("DONE", taskRec.status);

	}

	@Test
	public void test01CreateTask() throws Exception {
		String taskName = "TEST01-01";
		driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));

		Long taskId = driver.createTask(taskName, "test-data", "test-key1", "test-key2");

		TaskRec taskRec = driver.getContext().tedDao.getTask(taskId);
		assertEquals("NEW", taskRec.status);
		assertEquals(SYSTEM_ID, taskRec.system);
		assertEquals("TEST01-01", taskRec.name);
		//assertEquals("test-version", taskRec.version);
		assertEquals("test-data", new String(taskRec.data));

		assertEquals("test-key1", taskRec.key1);
		assertEquals("test-key2", taskRec.key2);
		assertEquals("MAIN", taskRec.channel);
		assertNotNull(taskRec.taskId);
		assertNotNull(taskRec.createTs);
		//assertNotNull(taskRec.startTs);
		assertNotNull(taskRec.nextTs);
		assertEquals(0, (int)taskRec.retries);
		assertNull(taskRec.startTs);
		assertNull(taskRec.msg);
		assertNull(taskRec.finishTs);
		//assertNull(taskRec.result);

		//driver.getContext().tedDao.setStatus(taskId, TedStatus.DONE, "test-msg", "test-result".getBytes());
		driver.getContext().tedDao.setStatus(taskId, TedStatus.DONE, "test-msg");
		taskRec = driver.getContext().tedDao.getTask(taskId);
		assertEquals("DONE", taskRec.status);
		assertEquals("test-msg", taskRec.msg);
		//assertEquals("test-result", new String(taskRec.result));

	}

	@Test
	public void test02CreateAndDone() throws Exception {
		String taskName = "TEST01-02";

		driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));


		Long taskId = driver.createTask(taskName, null, null, null);

		TaskRec taskRec = driver.getContext().tedDao.getTask(taskId);
		TestUtils.print(taskRec.toString());
		assertEquals("NEW", taskRec.status);

		// will start parallel
		driver.getContext().taskManager.processChannelTasks();

		taskRec = driver.getContext().tedDao.getTask(taskId);
		TestUtils.print(taskRec.toString());
		assertEquals("WORK", taskRec.status);

		TestUtils.sleepMs(500);

		// here we wait for time, not finish event, so sometimes it can fail
		taskRec = driver.getContext().tedDao.getTask(taskId);
		assertEquals("DONE", taskRec.status);

	}

	@Test
	public void test03CreateAndException() throws Exception {
		String taskName = "TEST01-03";

		driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorException.class));

		Long taskId = driver.createTask(taskName, null, null, null);

		TaskRec taskRec = driver.getContext().tedDao.getTask(taskId);
		TestUtils.print(taskRec.toString());
		assertEquals("NEW", taskRec.status);

		// will start parallel
		driver.getContext().taskManager.processChannelTasks();

		taskRec = driver.getContext().tedDao.getTask(taskId);
		TestUtils.print(taskRec.toString());
		assertEquals("WORK", taskRec.status);

		TestUtils.sleepMs(50);

		// here we wait for time, not finish event, so sometimes it can fail
		taskRec = driver.getContext().tedDao.getTask(taskId);
		assertEquals("ERROR", taskRec.status);
		assertEquals("Catch: Test runtime exception", taskRec.msg);

	}

	public static class Test01ProcessorRetry implements TedProcessor {
		@Override
		public TedResult process(TedTask task)  {
			logger.info(this.getClass().getSimpleName() + " process");
			TestUtils.sleepMs(20);
			return TedResult.retry("temporary problems");
		}
	}


	@Test
	public void test04CreateAndRetry() throws Exception {
		String taskName = "TEST01-04";

		driver.registerTaskConfig(taskName, TestTedProcessors.forClass(Test01ProcessorRetry.class));

		Long taskId = driver.createTask(taskName, null, null, null);

		TaskRec taskRec = driver.getContext().tedDao.getTask(taskId);
		//print(taskRec.toString());
		assertEquals("NEW", taskRec.status);

		// will start parallel
		driver.getContext().taskManager.processChannelTasks();

		taskRec = driver.getContext().tedDao.getTask(taskId);
		//print(taskRec.toString());
		assertEquals("WORK", taskRec.status);

		TestUtils.sleepMs(50);

		// here we wait for time, not finish event, so sometimes it can fail
		taskRec = driver.getContext().tedDao.getTask(taskId);
		long deltaMs = taskRec.nextTs.getTime() - new Date().getTime();
		TestUtils.print(taskRec.toString() + " deltaMs:" + deltaMs);
		assertEquals("RETRY", taskRec.status);
		assertTrue("next ts in 12 +/- 15% sec", (deltaMs > 9000 && deltaMs < 14000));
		assertEquals(1, (long)taskRec.retries);
	}


	@Test
	public void test05GetPortion() throws Exception {
		String taskName = "TEST01-05";
		driver.getContext().registry.registerChannel("TEST1", 5, 100);
		//driver.registerTaskConfig(taskName, forClass(Test01ProcessorRetry.class), 1, null, "TEST1");
		Properties taskProp = new Properties();

		driver.registerTaskConfig(taskName, TestTedProcessors.forClass(Test01ProcessorRetry.class));

		Long taskId = driver.createTask(taskName, null, null, null);

		TaskRec taskRec = driver.getContext().tedDao.getTask(taskId);
		//print(taskRec.toString());
		assertEquals("NEW", taskRec.status);


		Map<String, Integer> channelSizes = new HashMap<String, Integer>();
		channelSizes.put("TEST1", 3);
		List<TaskRec> list = driver.getContext().tedDao.reserveTaskPortion(channelSizes);
		assertEquals(1, list.size());
		assertEquals(taskName, list.get(0).name);
	}


	@Test
	public void test06GetPortionLocked() throws Exception {
		String taskName = "TEST01-05";
		dao_cleanupAllTasks();

		driver.getContext().registry.registerChannel("TEST1", 5, 100);
		driver.registerTaskConfig(taskName, TestTedProcessors.forClass(Test01ProcessorRetry.class));

		// create 2 tasks, then lock 1 of them
		driver.createTask(taskName, null, null, null);
		final Long lockTaskId = driver.createTask(taskName, null, null, null);
		TaskRec taskRec = driver.getContext().tedDao.getTask(lockTaskId);
		assertEquals("NEW", taskRec.status);

		logger.info("Before lock");
		new Thread(new Runnable() {
			@Override
			public void run() {
				dao_lockAndSleep(lockTaskId, "0.1");
				logger.info("After lock");
			}
		}).start();

		Thread.sleep(20);
		logger.info("Before next");

		Map<String, Integer> channelSizes = new HashMap<String, Integer>();
		channelSizes.put("TEST1", 3);
		List<TaskRec> list = driver.getContext().tedDao.reserveTaskPortion(channelSizes);
		assertEquals(1, list.size());
		assertEquals(taskName, list.get(0).name);
		logger.info("Done");

	}


/*	@Test
	public void test07CreateUniqueKey1() throws Exception {
		String taskName = "TEST01-07";

		driver.registerTaskConfig(taskName, TestUtils.forClass(Test01ProcessorRetry.class));

		Long taskId = driver.createTaskUniqueKey1(taskName, "row1", "key1", null);
		assertNotNull("should not created", taskId);
		taskId = driver.createTaskUniqueKey1(taskName, "row2", "key1", null);
		assertNull("should not be created due to key1 duplicate", taskId);
	}*/

	private void dao_lockAndSleep(long taskId, String sec) {
		if (context.tedDao instanceof TedDaoOracle) {
			((TedDaoOracle) context.tedDao).selectFromBlock("dao_lockAndSleep",
					"declare " +
					" v_till timestamp;" +
					" begin" +
						" update tedtask set status = status where taskid = " + taskId + ";" +
						" select systimestamp + interval '" + sec + "' second into v_till from dual;" +
						" loop " +
						"   exit when systimestamp >= v_till;" + // sleep uses cpu..
						" end loop;" +
					" end;",
					Void.class, Collections.<SqlParam>emptyList());
		} else {
			((TedDaoAbstract) context.tedDao).execute("dao_lockAndSleep",
					" update tedtask set status = status where taskid = " + taskId + "; SELECT pg_sleep(" + sec + ");", Collections.<SqlParam>emptyList());
		}
	}


}
