package com.github.ted.sys;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.ted.Ted.TedProcessor;
import com.github.ted.Ted.TedResult;
import com.github.ted.Ted.TedStatus;
import com.github.ted.Ted.TedTask;
import com.github.ted.sys.I01SimpleTest.Test01ProcessorOk;
import com.github.ted.sys.Model.TaskRec;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.github.ted.TedDriver.newTedTask;
import static com.github.ted.sys.TestConfig.SYSTEM_ID;

/**
 * @author Augustus
 *         created on 2017.09.15
 *
 *  creates a lot of tasks.
 */
@Ignore
public class I07BatchTest extends TestBase {
	private final static Logger logger = LoggerFactory.getLogger(I07BatchTest.class);

	private TedDriverImpl driver;
	private TedDao tedDao;

	@Override
	protected TedDriverImpl getDriver() { return driver; }

	@Before
	public void init() throws IOException {
		Properties properties = new Properties();
		String propFileName = "ted-I07.properties";
		InputStream inputStream = TestBase.class.getClassLoader().getResourceAsStream(propFileName);
		if (inputStream == null)
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		properties.load(inputStream);

		this.driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), SYSTEM_ID, properties);
		this.tedDao = driver.getContext().tedDao;
		//this.context = driver.getContext();
	}

	@Ignore
	@Test
	public void testCreate200() throws Exception {
		String taskName = "TEST01-01";
		driver.registerTaskConfig(taskName, TestUtils.forClass(Test01ProcessorOk.class));
		((TedDaoAbstract)driver.getContext().tedDao).getSequenceNextValue("SEQ_TEDTASK_BNO");

		long startTs = System.currentTimeMillis();
		for (int i = 0; i < 2000; i++) {
			driver.createTask(taskName, "test i07", "test1", "" + i);
		}
		// 200 items - 160, 182, 181
		// 2k - 1215, 898, 993
		TestUtils.log("create 200 in {0}s", System.currentTimeMillis() - startTs);

	}

	@Ignore
	@Test
	public void testCreateBulk200() throws Exception {
		String taskName = "TEST01-01";

		driver.registerTaskConfig(taskName, TestUtils.forClass(Test01ProcessorOk.class));
		((TedDaoAbstract)driver.getContext().tedDao).getSequenceNextValue("SEQ_TEDTASK_BNO");
		String param = "x";
		// for (int i = 0; i < 1000; i++) {
		// 	param += i + " ---------------------------------------------------------------------------------------------------- " + i + "\n";
		// }

		long startTs = System.currentTimeMillis();
		List<TedTask> taskParams = new ArrayList<TedTask>();
		for (int i = 0; i < 200; i++) {
			taskParams.add(newTedTask(taskName, "te\tst\\. i07" + param, "\\N", null));
		}
		driver.createTasksBulk(taskParams, null);
		// postgres.copy 200 - 35, 39, 41
		//    2k - 100, 102, 117
		TestUtils.log("create 200 in {0}s", System.currentTimeMillis() - startTs);

	}

	// create batch tasks, wait for finish (one of subtasks will retry once).
	// after subtasks finish, batch task processor will be executed and will return retry itself.
	@Test
	public void testBatch1() throws Exception {
		dao_cleanupAllTasks();

		String taskName = "TEST71";
		String batchName = "BAT07";
		driver.registerTaskConfig(taskName, TestUtils.forClass(ProcessorRandomOk.class));
		driver.registerTaskConfig(batchName, TestUtils.forClass(BatchFinishProcessor.class));

		List<TedTask> taskParams = new ArrayList<TedTask>();
		for (int i = 0; i < 3; i++) {
			taskParams.add(newTedTask(taskName, ""+i, null, null));
		}
		Long batchId = driver.createBatch(taskParams);

		TestUtils.sleepMs(500); // there can be difference between clocks in dev/tomcat and db server?
		driver.getContext().taskManager.processTasks();
		driver.getContext().taskManager.processTasks();
		TestUtils.sleepMs(20);
		Map<TedStatus, Integer> stats = tedDao.getBatchStatusStats(batchId);
		TestUtils.print(stats.toString());
		TaskRec batchRec = tedDao.getTask(batchId);
		Assert.assertEquals("batch should be RETRY until all task will be finished", "RETRY", batchRec.status);
		TestUtils.print("sleep...");

		TestUtils.sleepMs(1100 + 500); // wait 1s (retry)
		driver.getContext().taskManager.processTasks();
		TestUtils.sleepMs(100);

		batchRec = tedDao.getTask(batchId);
		Assert.assertEquals("batch should be RETRY because 1 task was delayed and not finished yet", "RETRY", batchRec.status);

		TestUtils.sleepMs(1000 + 500); // wait 1s (retry)
		driver.getContext().taskManager.processTasks();
		TestUtils.sleepMs(100);

		batchRec = tedDao.getTask(batchId);
		Assert.assertEquals("batch should be RETRY because even all tasks should be finished, but got RETRY from batch task", "RETRY", batchRec.status);
		Assert.assertEquals("retries should be cleanup after batch subtasks finished", 1L, batchRec.retries.longValue());

		stats = tedDao.getBatchStatusStats(batchId);

		TestUtils.print("batchTaskRec: " + batchRec.toString());
		TestUtils.print("statusStats: " + stats.toString());
	}

	// set second task to retry
	static class ProcessorRandomOk implements TedProcessor {
		@Override
		public TedResult process(TedTask task)  {
			logger.info(this.getClass().getSimpleName() + " process " + task.getData());
			TestUtils.sleepMs(20);
			if ("1".equals(task.getData())) {
				if (task.getRetries() == 0) {
					logger.info("Set task {} to RETRY", task.getTaskId());
					return TedResult.retry();
				} else { // next attempts
					logger.info("Set task {} to DONE", task.getTaskId());
					return TedResult.done();
				}
			}
			if ("2".equals(task.getData())) {
				logger.info("Set task {} to ERROR", task.getTaskId());
				return TedResult.error();
			}
			return TedResult.done();
		}
	}

	static class BatchFinishProcessor implements TedProcessor {
		@Override
		public TedResult process(TedTask task)  {
			logger.info(this.getClass().getSimpleName() + " process");
			//sleepMs(20);
			if (task.getRetries() == 0)
				return TedResult.retry("retry batch finish task");
			else
				return TedResult.done();
		}
	}



}
