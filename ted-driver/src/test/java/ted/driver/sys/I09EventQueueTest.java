package ted.driver.sys;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedDbType;
import ted.driver.Ted.TedProcessor;
import ted.driver.TedResult;
import ted.driver.TedTask;
import ted.driver.sys.JdbcSelectTed.SqlParam;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.QuickCheck.CheckResult;
import ted.driver.sys.TestTedProcessors.SingeInstanceFactory;
import ted.driver.sys.TestTedProcessors.TestProcessorFailAfterNDone;
import ted.driver.sys.TestTedProcessors.TestProcessorOk;
import ted.driver.sys.TestTedProcessors.TestProcessorOkSleep;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static ted.driver.sys.TestConfig.SYSTEM_ID;
import static ted.driver.sys.TestTedProcessors.forProcessor;
import static ted.driver.sys.TestUtils.print;
import static ted.driver.sys.TestUtils.sleepMs;

public class I09EventQueueTest extends TestBase {
	private final static Logger logger = LoggerFactory.getLogger(I09EventQueueTest.class);

	private TedDriverImpl driver;
	private TedDao tedDao;

	private Random random = new Random();

	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

	@Override
	protected TedDriverImpl getDriver() { return driver; }

	@Before
	public void init() throws IOException {
		Assume.assumeTrue("For PostgreSQL only", TestConfig.testDbType == TedDbType.POSTGRES);

		Properties properties = TestUtils.readPropertiesFile("ted-I09.properties");
		this.driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), SYSTEM_ID, properties);
		this.tedDao = driver.getContext().tedDao;
		//this.context = driver.getContext();

	}

	private void dao_execSql (String sql) {
		((TedDaoAbstract)getContext().tedDao).execute("test", sql, Collections.<SqlParam>emptyList());
	}

	private Long dao_selectLong (String sql) {
		return ((TedDaoAbstract)getContext().tedDao).selectSingleLong("test", sql);
	}

	@Test
	public void test01TakeFirst() {
		String taskName = "TEST09-1";
		cleanupData();

		Long taskId = driver.createEvent(taskName, "test9-a", "task1" , null);
		TaskRec taskRec = tedDao.getTask(taskId);
		print(taskRec.toString());
		assertEquals("NEW", taskRec.status);
		dao_execSql("update tedtask set status = 'SLEEP' where system = '" + SYSTEM_ID + "' and taskId = " + taskId);

		// the first task should be NEW after creation of task2
		Long taskId2 = driver.createEvent(taskName, "test9-a", "task2" , null);
		assertEquals("NEW", tedDao.getTask(taskId).status);
		assertEquals("SLEEP", tedDao.getTask(taskId2).status);

	}
	@Test
	public void test02TryExecute() {
		String taskName = "TEST09-1";
		dao_execSql("update tedtask set status = 'DONE', nextts = null where system = '" + SYSTEM_ID + "' and channel = 'TedEQ' " +
				" and status <> 'DONE'");
		driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));
		Long taskId = driver.createEventAndTryExecute(taskName, "test9-a", "task1" , null);
		sleepMs(50);
		TaskRec taskRec = tedDao.getTask(taskId);
		print(taskRec.toString());
		assertEquals("DONE", taskRec.status);
	}

	@Test
	public void test03EventStatuses() {
		String taskName = "TEST09-1";
		String taskName2 = "TEST09-2";
		cleanupData();

		driver.registerTaskConfig(taskName, new SingeInstanceFactory(new TestProcessorFailAfterNDone(1, TedResult.error("error"))));
		driver.registerTaskConfig(taskName2, new SingeInstanceFactory(new TestProcessorFailAfterNDone(1, TedResult.retry("retry"))));

		// first must become NEW
		Long taskId = driver.createEvent(taskName, "test9-1", "abra1" , null);
		TaskRec taskRec = tedDao.getTask(taskId);
		print(taskRec.toString());
		assertEquals("NEW", taskRec.status);

		// second and others must become SLEEP
		Long taskId2 = driver.createEvent(taskName, "test9-1", "abra2" , null);
		taskRec = tedDao.getTask(taskId2);
		print(taskRec.toString());
		assertEquals("SLEEP", taskRec.status);

		Long taskId3 = driver.createEvent(taskName, "test9-1", "abra2" , null);

		// by other queueId - again first NEW
		Long taskId4 = driver.createEvent(taskName2, "test9-2", "abra2" , null);
		taskRec = tedDao.getTask(taskId4);
		print(taskRec.toString());
		assertEquals("NEW", taskRec.status); // first must become NEW

		Long taskId5 = driver.createEvent(taskName2, "test9-2", "abra2" , null);
		Long taskId6 = driver.createEvent(taskName2, "test9-2", "abra2" , null);

		sleepMs(20);
		driver.getContext().eventQueueManager.processTedQueue();

		sleepMs(100);

		assertEquals("DONE", tedDao.getTask(taskId).status);
		assertEquals("ERROR", tedDao.getTask(taskId2).status);
		assertEquals("SLEEP", tedDao.getTask(taskId3).status);

		assertEquals("DONE", tedDao.getTask(taskId4).status);
		assertEquals("RETRY", tedDao.getTask(taskId5).status);
		assertEquals("SLEEP", tedDao.getTask(taskId6).status);

	}

	@Test
	public void test04EventCreateEvent() {
		String taskName = "TEST09-1";
		final String taskName2 = "TEST09-2";
		cleanupData();

		final Long[] taskId2 = new Long[2];
		driver.registerTaskConfig(taskName, new SingeInstanceFactory(new TedProcessor() {
				@Override
				public TedResult process(TedTask task) {
					taskId2[0] = getContext().tedDriver.createEventAndTryExecute(taskName2, "abra", "2", null);
					taskId2[1] = getContext().tedDriver.createEvent(taskName2, "abra", "3", null);
					return TedResult.done();
				}
			}
		));
		driver.registerTaskConfig(taskName2, TestTedProcessors.forClass(TestProcessorOk.class));

		// first must become NEW
		Long taskId = driver.createEvent(taskName, "abra", "abra1" , null);
		TaskRec taskRec = tedDao.getTask(taskId);
		print(taskRec.toString());
		assertEquals("NEW", taskRec.status);

		sleepMs(20);
		driver.getContext().eventQueueManager.processTedQueue();

		sleepMs(220);
		assertEquals("task1:" + taskId,"DONE", tedDao.getTask(taskId).status);
		assertEquals("task2:" + taskId2[0], "DONE", tedDao.getTask(taskId2[0]).status);
	}

	@Test
	public void test05CreateEventAndTryExecute() {
		String taskName = "TEST09-5";
		cleanupData();

		final String key2 = "ted.test9-5-" + Integer.toString(Math.abs(random.nextInt()), 36);

		driver.registerTaskConfig(taskName, forProcessor(new TestProcessorOkSleep(300)));

		new Thread(new Runnable() {
			@Override
			public void run() {
				logger.debug("start thread, will sleep for 150ms");
				sleepMs(100); // need to start after createEventAndTryExecute
				driver.getContext().eventQueueManager.processTedQueue();
				// should be skipped
				Long taskId = dao_selectLong("select taskid from tedtask where system = '" + SYSTEM_ID + "' and key2='" + key2 +"'");
				TaskRec taskRec = tedDao.getTask(taskId);
				print(taskRec.toString());
				assertEquals("WORK", taskRec.status);
			}
		}).start();

		// first must become NEW
		long taskId = driver.createEventAndTryExecute(taskName, "test5", "abra1" , key2);
		TaskRec taskRec = tedDao.getTask(taskId);
		print(taskRec.toString());
		assertEquals("DONE", taskRec.status);
	}


	@Ignore
	@Test
	public void test11EventQueue50() {
		String taskName = "TEST09-3";
		cleanupData();

		driver.registerTaskConfig(taskName, new SingeInstanceFactory(new TestProcessorFailAfterNDone(3, TedResult.retry("error"))));

		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < 5; j++) {
				driver.createEvent(taskName, "abra-" + j, "abra-" + i + "-" + j , null);
			}
		}
		Long taskId = driver.createEvent(taskName, "abra", "abra-data", null);
		List<CheckResult> res = tedDao.quickCheck(null, false);
		print(gson.toJson(res));


		sleepMs(20);
		// will start parallel
		driver.getContext().eventQueueManager.processTedQueue();

		sleepMs(2000);
		// here we wait for time, not finish event, so sometimes it can fail
		TaskRec taskRec = driver.getContext().tedDao.getTask(taskId);
		assertEquals("DONE", taskRec.status);
	}

	private void cleanupData() {
		dao_execSql("update tedtask set status = 'DONE', nextts = null where system = '" + SYSTEM_ID + "' and channel = 'TedEQ' " +
				" and status <> 'DONE'");
	}
}
