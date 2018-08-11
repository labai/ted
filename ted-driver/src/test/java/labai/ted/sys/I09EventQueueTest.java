package labai.ted.sys;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import labai.ted.Ted.TedDbType;
import labai.ted.Ted.TedProcessor;
import labai.ted.TedResult;
import labai.ted.TedTask;
import labai.ted.sys.JdbcSelectTed.SqlParam;
import labai.ted.sys.Model.TaskRec;
import labai.ted.sys.QuickCheck.CheckResult;
import labai.ted.sys.TestTedProcessors.SingeInstanceFactory;
import labai.ted.sys.TestTedProcessors.TestProcessorFailAfterNDone;
import labai.ted.sys.TestTedProcessors.TestProcessorOk;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static labai.ted.sys.TestConfig.SYSTEM_ID;
import static labai.ted.sys.TestTedProcessors.forClass;
import static labai.ted.sys.TestUtils.print;
import static labai.ted.sys.TestUtils.sleepMs;
import static org.junit.Assert.assertEquals;

public class I09EventQueueTest extends TestBase {
	private final static Logger logger = LoggerFactory.getLogger(I09EventQueueTest.class);

	private TedDriverImpl driver;
	private TedDao tedDao;

	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

	@Override
	protected TedDriverImpl getDriver() { return driver; }

	@Before
	public void init() throws IOException {
		Assume.assumeTrue("Not for Oracle", TestConfig.testDbType == TedDbType.POSTGRES);

		Properties properties = TestUtils.readPropertiesFile("ted-I09.properties");
		this.driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), SYSTEM_ID, properties);
		this.tedDao = driver.getContext().tedDao;
		//this.context = driver.getContext();

	}

	private void dao_execSql (String sql) {
		((TedDaoAbstract)getContext().tedDao).execute("test", sql, Collections.<SqlParam>emptyList());
	}
	@Test
	public void test01TakeFirst() {
		String taskName = "TEST09-1";
		dao_execSql("update tedtask set status = 'DONE' where system = '" + SYSTEM_ID + "' and channel = 'TedEQ' " +
				" and status <> 'DONE'");

		Long taskId = driver.createEvent(taskName, "test9-a", "task1" , null);
		TaskRec taskRec = tedDao.getTask(taskId);
		TestUtils.print(taskRec.toString());
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
		dao_execSql("update tedtask set status = 'DONE' where system = '" + SYSTEM_ID + "' and channel = 'TedEQ' " +
				" and status <> 'DONE'");
		driver.registerTaskConfig(taskName, forClass(TestProcessorOk.class));
		Long taskId = driver.createAndTryExecuteEvent(taskName, "test9-a", "task1" , null);
		sleepMs(50);
		TaskRec taskRec = tedDao.getTask(taskId);
		TestUtils.print(taskRec.toString());
		assertEquals("DONE", taskRec.status);
	}

	@Test
	public void test03EventStatuses() {
		String taskName = "TEST09-1";
		String taskName2 = "TEST09-2";
		dao_execSql("update tedtask set status = 'DONE' where system = '" + SYSTEM_ID + "' and channel = 'TedEQ' " +
				" and status <> 'DONE'");
		driver.registerTaskConfig(taskName, new SingeInstanceFactory(new TestProcessorFailAfterNDone(1, TedResult.error("error"))));
		driver.registerTaskConfig(taskName2, new SingeInstanceFactory(new TestProcessorFailAfterNDone(1, TedResult.retry("retry"))));

		// first must become NEW
		Long taskId = driver.createEvent(taskName, "test9-1", "abra1" , null);
		TaskRec taskRec = tedDao.getTask(taskId);
		TestUtils.print(taskRec.toString());
		assertEquals("NEW", taskRec.status);

		// second and others must become SLEEP
		Long taskId2 = driver.createEvent(taskName, "test9-1", "abra2" , null);
		taskRec = tedDao.getTask(taskId2);
		TestUtils.print(taskRec.toString());
		assertEquals("SLEEP", taskRec.status);

		Long taskId3 = driver.createEvent(taskName, "test9-1", "abra2" , null);

		// by other queueId - again first NEW
		Long taskId4 = driver.createEvent(taskName2, "test9-2", "abra2" , null);
		taskRec = tedDao.getTask(taskId4);
		TestUtils.print(taskRec.toString());
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
		dao_execSql("update tedtask set status = 'DONE' where system = '" + SYSTEM_ID + "' and channel = 'TedEQ' " +
				" and status <> 'DONE'");
		final Long[] taskId2 = new Long[1];
		driver.registerTaskConfig(taskName, new SingeInstanceFactory(new TedProcessor() {
				@Override
				public TedResult process(TedTask task) {
					taskId2[0] = getContext().tedDriver.createEvent(taskName2, "abra", "2", null);
					return TedResult.done();
				}
			}
		));
		driver.registerTaskConfig(taskName2, forClass(TestProcessorOk.class));

		// first must become NEW
		Long taskId = driver.createEvent(taskName, "abra", "abra1" , null);
		TaskRec taskRec = tedDao.getTask(taskId);
		TestUtils.print(taskRec.toString());
		assertEquals("NEW", taskRec.status);

		sleepMs(20);
		driver.getContext().eventQueueManager.processTedQueue();

		sleepMs(100);
		assertEquals("DONE", tedDao.getTask(taskId).status);
		assertEquals("DONE", tedDao.getTask(taskId2[0]).status);


	}

	@Ignore
	@Test
	public void test11EventQueue50() {
		String taskName = "TEST09-3";
		dao_execSql("update tedtask set status = 'DONE' where system = '" + SYSTEM_ID + "' and channel = 'TedEQ' " +
				" and status <> 'DONE'");

		driver.registerTaskConfig(taskName, new SingeInstanceFactory(new TestProcessorFailAfterNDone(3, TedResult.retry("error"))));

		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < 5; j++) {
				driver.createEvent(taskName, "abra-" + j, "abra-" + i + "-" + j , null);
			}
		}
		Long taskId = driver.createEvent(taskName, "abra", "abra-data", null);
		List<CheckResult> res = tedDao.quickCheck(null);
		print(gson.toJson(res));


		sleepMs(20);
		// will start parallel
		driver.getContext().eventQueueManager.processTedQueue();

		TestUtils.sleepMs(2000);
		// here we wait for time, not finish event, so sometimes it can fail
		TaskRec taskRec = driver.getContext().tedDao.getTask(taskId);
		assertEquals("DONE", taskRec.status);


	}
}
