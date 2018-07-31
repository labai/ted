package com.github.labai.ted.sys;

import com.github.labai.ted.Ted.TedProcessor;
import com.github.labai.ted.Ted.TedResult;
import com.github.labai.ted.Ted.TedTask;
import com.github.labai.ted.sys.JdbcSelectTed.JetJdbcParamType;
import com.github.labai.ted.sys.Model.TaskRec;
import com.github.labai.ted.sys.TedDriverImpl.TedContext;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static com.github.labai.ted.sys.JdbcSelectTed.sqlParam;
import static com.github.labai.ted.sys.TestConfig.SYSTEM_ID;
import static com.github.labai.ted.sys.TestUtils.print;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author Augustus
 *         created on 2016.09.19
 */
public class I03MaintenanceTest extends TestBase {
	private final static Logger logger = LoggerFactory.getLogger(I03MaintenanceTest.class);

	private TedDriverImpl driver;
	private TedContext context;

	@Override
	protected TedDriverImpl getDriver() { return driver; }

	@Before
	public void init() throws IOException {
		Properties properties = new Properties();
		String propFileName = "ted-I03.properties";
		InputStream inputStream = TestBase.class.getClassLoader().getResourceAsStream(propFileName);
		if (inputStream == null)
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		properties.load(inputStream);

		this.driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), SYSTEM_ID, properties);
		this.context = driver.getContext();
	}


	public static class Test01ProcessorOk implements TedProcessor {
		@Override
		public TedResult process(TedTask task)  {
			logger.info(this.getClass().getSimpleName() + " process");
			TestUtils.sleepMs(50);
			return TedResult.done();
		}
	}


	private void dao_setDoneAndOld(long taskId, int daysBack) {
		long delta = daysBack * 3600 * 24 * 1000;
		Date longAgo = new Date(System.currentTimeMillis() - delta);
		print("longAgo:" + longAgo);
		((TedDaoAbstract)context.tedDao).execute("dao_setDoneAndOld",
				" update tedtask set status = 'DONE', createTs = ?, finishTs = ? where taskId = ?", asList(
						sqlParam(longAgo, JetJdbcParamType.TIMESTAMP),
						sqlParam(longAgo, JetJdbcParamType.TIMESTAMP),
						sqlParam(taskId, JetJdbcParamType.LONG)
				));
	}
	private void dao_setStartTs(long taskId, Date startTs) {
		((TedDaoAbstract)context.tedDao).execute("dao_setStartTs",
				" update tedtask set startTs = ? where taskId = ?", asList(
					sqlParam(startTs, JetJdbcParamType.TIMESTAMP),
					sqlParam(taskId, JetJdbcParamType.LONG)
				));
	}
	private void dao_setCreateTs(long taskId, Date createTs) {
		((TedDaoAbstract)context.tedDao).execute("dao_setCreateTs",
				" update tedtask set createTs = ? where taskId = ?", asList(
						sqlParam(createTs, JetJdbcParamType.TIMESTAMP),
						sqlParam(taskId, JetJdbcParamType.LONG)
				));
	}
	private void dao_setNextTs(long taskId, Date nextTs) {
		((TedDaoAbstract)context.tedDao).execute("dao_setNextTs",
				" update tedtask set nextTs = ? where taskId = ?", asList(
						sqlParam(nextTs, JetJdbcParamType.TIMESTAMP),
						sqlParam(taskId, JetJdbcParamType.LONG)
				));
	}


	@Test
	public void test01WorkTimeoutMinute() throws Exception {
		String taskName = "TEST03-01";
		dao_cleanupAllTasks();

		driver.registerTaskConfig(taskName, TestUtils.forClass(Test01ProcessorOk.class), 1, null, Model.CHANNEL_MAIN);
		Long taskId;
		TaskRec taskRec;

		driver.createTask(taskName, null, null, null);
		TestUtils.sleepMs(10);
		// set status to WORK
		List<TaskRec> tasks = context.tedDao.reserveTaskPortion(new HashMap<String, Integer>() {{ put(Model.CHANNEL_MAIN, 1); }});
		taskId = tasks.get(0).taskId;

		taskRec = context.tedDao.getTask(taskId);
		//print(taskRec.toString() + " startTs=" + taskRec.startTs);
		assertEquals("WORK", taskRec.status);

		// change startTs to test work timeout
		dao_setStartTs(taskId, new Date(new Date().getTime() - 121 * 1000));
		taskRec = context.tedDao.getTask(taskId);
		//print(taskRec.toString() + " startTs=" + taskRec.startTs);

		context.taskManager.processMaintenanceTasks();

		// task should be canceled due to timeout
		taskRec = context.tedDao.getTask(taskId);
		//print(taskRec.toString());
		assertEquals("RETRY", taskRec.status);
		assertEquals("Too long in status [work](3)", taskRec.msg);

		assertFalse("is not new", taskRec.getTedTask().isNew());
		assertTrue("is retry", taskRec.getTedTask().isRetry());
		assertTrue("is timeout", taskRec.getTedTask().isAfterTimeout());
	}


	// if task TEST03-02 working > 1 min but < 40 min (setup in config), then finishTs should be set to this task.
	@Test
	public void test02WorkTimeoutPostpone() throws Exception {
		String taskName = "TEST03-02";
		dao_cleanupAllTasks();

		// with timeout 40 minutes
		driver.registerTaskConfig(taskName, TestUtils.forClass(Test01ProcessorOk.class));
		//driver.registerTaskConfig(taskName, forClass(Test01ProcessorOk.class), 40, null, "MAIN");
		Long taskId;
		TaskRec taskRec;

		driver.createTask(taskName, null, null, null);

		//driver.getContext().config.defaultTaskTimeoutMn = 30 * 60; // default - 30min

		// set status to work
		List<TaskRec> tasks = context.tedDao.reserveTaskPortion(new HashMap<String, Integer>(){{put("MAIN",1);}});
		taskId = tasks.get(0).taskId;

		taskRec = context.tedDao.getTask(taskId);
		//print(taskRec.toString() + " startTs=" + taskRec.startTs);
		assertEquals("WORK", taskRec.status);
		assertNull(taskRec.finishTs); // finish time is not set in beginning

		// 1. 30 min - still working
		dao_setStartTs(taskId, new Date(new Date().getTime() - 31 * 60 * 1000));
		context.taskManager.processMaintenanceTasks();
		taskRec = context.tedDao.getTask(taskId);
		print(taskRec.toString() + " startTs=" + MiscUtils.toTimeString(taskRec.startTs));
		assertEquals("WORK", taskRec.status);
		assertNotNull(taskRec.finishTs); // after processMaintenanceTasks finishTs must be set to startTs + 40 min
		dao_cleanupTasks(taskName);

	}

	// unknown tasks will be postponed for 2 minutes, but after 1 day will be canceled
	@Test
	public void test02UnknownPostpone() throws Exception {
		String taskName = "TEST03-02XX";
		dao_cleanupAllTasks();
		Long taskId;
		TaskRec taskRec;

		// create unknown task
		TedDriverImpl driverTmp = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), SYSTEM_ID, new Properties());
		driverTmp.registerTaskConfig(taskName, TestUtils.forClass(Test01ProcessorOk.class), 40, null, "MAIN");
		taskId = driverTmp.createTask(taskName, null, null, null);

		// process unknown task
		context.taskManager.processTasks();
		Thread.sleep(20);

		// 1. Must be postponed (new with some nextts > now())
		taskRec = context.tedDao.getTask(taskId);
		print(taskRec.toString() + " startTs=" + MiscUtils.toTimeString(taskRec.startTs));
		assertEquals("NEW", taskRec.status);

		// 2. after 1 day - must go to error
		print("Setting createTs to > 1 day ago");
		dao_setCreateTs(taskId, new Date(new Date().getTime() - 24 * 60 * 61 * 1000));
		dao_setNextTs(taskId, new Date(new Date().getTime() - 1 * 1000));

		context.taskManager.processTasks();
		Thread.sleep(20);

		taskRec = context.tedDao.getTask(taskId);
		print(taskRec.toString() + " startTs=" + MiscUtils.toTimeString(taskRec.startTs));
		assertEquals("ERROR", taskRec.status);

		dao_cleanupTasks(taskName);

	}

	// if task TEST03-02 working > 40 min (setup in config), then it should be set to ERROR
	@Test
	public void test02WorkTimeoutCancel() throws Exception {
		String taskName = "TEST03-02";
		dao_cleanupAllTasks();

		// with timeout 40 minutes
		driver.registerTaskConfig(taskName, TestUtils.forClass(Test01ProcessorOk.class));

		Long taskId;
		TaskRec taskRec;

		driver.createTask(taskName, null, null, null);

		// set status to work
		List<TaskRec> tasks = context.tedDao.reserveTaskPortion(new HashMap<String, Integer>(){{put("MAIN",1);}});
		taskId = tasks.get(0).taskId;

		taskRec = context.tedDao.getTask(taskId);
		//print(taskRec.toString() + " startTs=" + taskRec.startTs);
		assertEquals("WORK", taskRec.status);
		assertNull(taskRec.finishTs); // finish time is not set in beginning

		// change startTs to test work timeout - more than 40 minutes - should go to ERROR
		dao_setStartTs(taskId, new Date(new Date().getTime() - 41 * 60 * 1000));
		taskRec = context.tedDao.getTask(taskId);
		print(taskRec.toString() + " startTs=" + MiscUtils.toTimeString(taskRec.startTs));
		context.taskManager.processMaintenanceTasks();
		taskRec = context.tedDao.getTask(taskId);
		assertEquals("RETRY", taskRec.status);
		assertEquals("Too long in status [work](3)", taskRec.msg);


	}


	@Test
	public void test03DeleteOldTasks() throws Exception {
		// oldTaskArchiveDays = 5

		String taskName = "TEST03-03";
		dao_cleanupAllTasks();

		driver.registerTaskConfig(taskName, TestUtils.forClass(Test01ProcessorOk.class));

		Long taskId = driver.createTask(taskName, null, null, null);

		// check not so old tasks (should not be deleted)
		//
		dao_setDoneAndOld(taskId, 4);

		TaskRec taskRec = context.tedDao.getTask(taskId);
		//print(taskRec.toString() + " startTs=" + taskRec.startTs);
		assertEquals("DONE", taskRec.status);
		assertEquals(5, driver.getContext().config.oldTaskArchiveDays()); // from config
		context.tedDao.processMaintenanceRare(driver.getContext().config.oldTaskArchiveDays());

		taskRec = context.tedDao.getTask(taskId); // should still exists

		// check older tasks (should be deleted)
		//
		dao_setDoneAndOld(taskId, 6);
		context.tedDao.processMaintenanceRare(driver.getContext().config.oldTaskArchiveDays());

		try {
			taskRec = context.tedDao.getTask(taskId);
			fail("Expected exception as not task should exists");
		} catch (Exception e) {
		}

	}

}
