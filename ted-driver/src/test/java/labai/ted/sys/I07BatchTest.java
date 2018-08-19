package labai.ted.sys;

import labai.ted.Ted.TedProcessor;
import labai.ted.Ted.TedStatus;
import labai.ted.TedDriver;
import labai.ted.TedResult;
import labai.ted.TedTask;
import labai.ted.sys.JdbcSelectTed.SqlParam;
import labai.ted.sys.Model.TaskRec;
import labai.ted.sys.TestTedProcessors.TestProcessorOk;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static labai.ted.sys.TestConfig.SYSTEM_ID;
import static labai.ted.sys.TestUtils.print;
import static org.junit.Assert.assertEquals;

/**
 * @author Augustus
 *         created on 2017.09.15
 *
 *  creates a lot of tasks.
 */
public class I07BatchTest extends TestBase {
	private final static Logger logger = LoggerFactory.getLogger(I07BatchTest.class);

	private TedDriverImpl driver;
	private TedDao tedDao;

	@Override
	protected TedDriverImpl getDriver() { return driver; }

	@Before
	public void init() throws IOException {
		Properties properties = TestUtils.readPropertiesFile("ted-I07.properties");
		this.driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), SYSTEM_ID, properties);
		this.tedDao = driver.getContext().tedDao;
		//this.context = driver.getContext();
	}

	@Ignore
	@Test
	public void testCreate200() throws Exception {
		String taskName = "TEST01-01";
		driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));
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

		driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));
		((TedDaoAbstract)driver.getContext().tedDao).getSequenceNextValue("SEQ_TEDTASK_BNO");
		String param = "x";
		// for (int i = 0; i < 1000; i++) {
		// 	param += i + " ---------------------------------------------------------------------------------------------------- " + i + "\n";
		// }

		long startTs = System.currentTimeMillis();
		List<TedTask> taskParams = new ArrayList<TedTask>();
		for (int i = 0; i < 200; i++) {
			taskParams.add(TedDriver.newTedTask(taskName, "te\tst\\. i07" + param, "\\N", null));
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

		String taskName = "TEST07-1";
		String batchName = "BAT07";
		driver.registerTaskConfig(taskName, TestTedProcessors.forClass(ProcessorRandomOk.class));
		driver.registerTaskConfig(batchName, TestTedProcessors.forClass(BatchFinishProcessor.class));

		List<TedTask> taskParams = new ArrayList<TedTask>();
		for (int i = 0; i < 3; i++) {
			taskParams.add(TedDriver.newTedTask(taskName, ""+i, null, null));
		}
		Long batchId = driver.createBatch(batchName, "data", "key1", "key2", taskParams);

		setTaskNextTsNow(batchId); // there can be difference between clocks in dev/tomcat and db server?..
		TestUtils.sleepMs(10);
		driver.getContext().taskManager.processChannelTasks();
		driver.getContext().batchWaitManager.processBatchWaitTasks();
		TestUtils.sleepMs(300);
		Map<TedStatus, Integer> stats = tedDao.getBatchStatusStats(batchId);
		print(stats.toString());
		TaskRec batchRec = tedDao.getTask(batchId);
		print(batchRec.toString());
		print(batchRec.getTedTask().toString());
		assertEquals("batch should be RETRY until all task will be finished", "RETRY", batchRec.status);
		assertEquals("Batch task is waiting for finish of subtasks", batchRec.msg);
		print("sleep...");

		TestUtils.sleepMs(1100 + 1500); // wait 1s (retry)
		driver.getContext().taskManager.processChannelTasks(); // here all subtask should be finished
		TestUtils.sleepMs(50);
		print(tedDao.getBatchStatusStats(batchId).toString());

		driver.getContext().batchWaitManager.processBatchWaitTasks();
		TestUtils.sleepMs(120);

		batchRec = tedDao.getTask(batchId);
		print(batchRec.toString());
		assertEquals("batch should be RETRY because 1 task was delayed and not finished yet", "RETRY", batchRec.status);
		assertEquals("channel should be as is in config", "BAT", batchRec.channel);

		print("subtasks finished, waiting for batch tasks own retry");
		TestUtils.sleepMs(1000 + 1500); // wait 1s (retry)
		driver.getContext().taskManager.processChannelTasks();
		TestUtils.sleepMs(100);

		batchRec = tedDao.getTask(batchId);
		print(batchRec.toString());
		assertEquals("batch should be finished", "DONE", batchRec.status);
		assertEquals("retries should be cleanup after batch subtasks finished", 1L, batchRec.retries.longValue());

		stats = tedDao.getBatchStatusStats(batchId);

		print("batchTaskRec: " + batchRec.toString());
		print("statusStats: " + stats.toString());
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

	private void setTaskNextTsNow(Long taskId) {
		String sql = "update tedtask set nextts = $now where taskId=" + taskId
			+ " and status in ('NEW', 'RETRY')";
		sql = sql.replace("$now", tedDao.getDbType().sql.now());
		((TedDaoAbstract)tedDao).execute("setTaskNextTsNow", sql, Collections.<SqlParam>emptyList());
	}

}
