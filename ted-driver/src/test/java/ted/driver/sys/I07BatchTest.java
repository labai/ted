package ted.driver.sys;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedProcessor;
import ted.driver.TedDriver;
import ted.driver.TedResult;
import ted.driver.TedTask;
import ted.driver.sys.JdbcSelectTed.JetJdbcParamType;
import ted.driver.sys.JdbcSelectTed.SqlParam;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.TestTedProcessors.TestProcessorOk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static ted.driver.sys.JdbcSelectTed.sqlParam;
import static ted.driver.sys.TestUtils.print;
import static ted.driver.sys.TestUtils.sleepMs;

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
		this.driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID, properties);
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
		String batchChannel = "BAT1";

		driver.registerTaskConfig(taskName, TestTedProcessors.forClass(ProcessorRandomOk.class));
		driver.registerTaskConfig(batchName, TestTedProcessors.forClass(BatchFinishProcessor.class));

		List<TedTask> taskParams = new ArrayList<TedTask>();
		for (int i = 0; i < 3; i++) {
			taskParams.add(TedDriver.newTedTask(taskName, ""+i, null, null));
		}
		final Long batchId = driver.createBatch(batchName, "data", "key1", "key2", taskParams);

		final List<TaskRec> tasks = getBatchTasks(batchId);


		setTaskNextTsNow(batchId); // there can be difference between clocks in dev/tomcat and db server?..
		sleepMs(10);
		driver.getContext().taskManager.processChannelTasks();
		driver.getContext().batchWaitManager.processBatchWaitTasks();
		sleepMs(50);

		TestUtils.awaitUntilTaskFinish(driver, batchId, 200);

		TaskRec batchRec = tedDao.getTask(batchId);
		print(batchRec.toString());
		print(batchRec.getTedTask().toString());
		assertEquals("batch should be RETRY until all task will be finished", "RETRY", batchRec.status);
		assertEquals("Batch task is waiting for finish of subtasks", batchRec.msg);
		print("sleep...");

		await().atMost(2600, TimeUnit.MILLISECONDS).pollInterval(TestUtils.POLL_INTERVAL).until(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				driver.getContext().taskManager.processChannelTasks(); // here all subtask should be finished
				List<String> finished = asList("DONE", "ERROR");
				for (TaskRec rec : tasks) {
					TaskRec rec2 = driver.getContext().tedDao.getTask(rec.taskId);
					logger.debug("task {} status {}", rec2.taskId, rec2.status);
					if (! finished.contains(rec2.status))
						return false;
				}
				return true;
			}
		});

		driver.getContext().taskManager.processChannelTasks(); // here all subtask should be finished
		sleepMs(50);

		driver.getContext().batchWaitManager.processBatchWaitTasks();
		sleepMs(50);

		batchRec = tedDao.getTask(batchId);
		print(batchRec.toString());
		assertEquals("batch should be RETRY because 1 task was delayed and not finished yet", "RETRY", batchRec.status);
		assertEquals("channel should be as is in config", batchChannel, batchRec.channel);

		print("subtasks finished, waiting for batch tasks own retry");
		await().atMost(2600, TimeUnit.MILLISECONDS).pollInterval(TestUtils.POLL_INTERVAL).until(new Callable<Boolean>() {
			@Override
			public Boolean call() {
				driver.getContext().taskManager.processChannelTasks();
				TaskRec rec = driver.getContext().tedDao.getTask(batchId);
				return asList("DONE", "ERROR").contains(rec.status);
			}
		});

		batchRec = tedDao.getTask(batchId);
		print(batchRec.toString());
		assertEquals("batch should be finished", "DONE", batchRec.status);
		assertEquals("retries should be cleanup after batch subtasks finished", 1L, batchRec.retries.longValue());

		print("batchTaskRec: " + batchRec.toString());
	}

	// set second task to retry
	static class ProcessorRandomOk implements TedProcessor {
		@Override
		public TedResult process(TedTask task)  {
			logger.info(this.getClass().getSimpleName() + " process " + task.getData());
			sleepMs(20);
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


	private List<TaskRec> getBatchTasks(Long batchId) {
		String sql = "select * from tedtask where batchid = ?";
		List<TaskRec> results = ((TedDaoAbstract)tedDao).selectData("batchIds", sql, TaskRec.class, asList(
				sqlParam(batchId, JetJdbcParamType.LONG)
		));
		return results;	}

}
