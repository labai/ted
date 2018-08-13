package labai.ted.sys;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import labai.ted.Ted.TedDbType;
import labai.ted.TedResult;
import labai.ted.TedTask;
import labai.ted.sys.JdbcSelectTed.SqlParam;
import labai.ted.sys.Model.TaskRec;
import labai.ted.sys.TestTedProcessors.SingeInstanceFactory;
import labai.ted.sys.TestTedProcessors.TestProcessorOk;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

import static labai.ted.sys.TestConfig.SYSTEM_ID;
import static labai.ted.sys.TestUtils.sleepMs;
import static org.junit.Assert.assertEquals;

public class I10NotificationTest extends TestBase {
	private final static Logger logger = LoggerFactory.getLogger(I10NotificationTest.class);

	private TedDriverImpl driver1;
	private TedDriverImpl driver2;

	private TedDao tedDao;

	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

	private static int counter = 0;

	@Override
	protected TedDriverImpl getDriver() { return driver1; }

	@Before
	public void init() throws IOException {
		Assume.assumeTrue("Not for Oracle", TestConfig.testDbType == TedDbType.POSTGRES);

		driver1 = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID);
		driver2 = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID);

		tedDao = driver1.getContext().tedDao;

	}

	private void dao_execSql (String sql) {
		((TedDaoAbstract)getContext().tedDao).execute("test", sql, Collections.<SqlParam>emptyList());
	}

	@Test
	public void test01SendNotification() {
		String taskName = "TEST10-1";
		dao_execSql("update tedtask set status = 'DONE', nextts = null where system = '" + SYSTEM_ID + "' and channel = 'TedIN' " +
				" and status <> 'DONE'");
		final int[] calls = {0, 0};
		driver1.registerTaskConfig(taskName, new SingeInstanceFactory(new TestProcessorOk(){
			@Override
			public TedResult process(TedTask task) {
				calls[0]++;
				return super.process(task);
			}
		}));
		driver2.registerTaskConfig(taskName, new SingeInstanceFactory(new TestProcessorOk(){
			@Override
			public TedResult process(TedTask task) {
				calls[1]++;
				return super.process(task);
			}
		}));
		driver1.prime().enable();
		driver1.prime().init();
		driver2.prime().enable();
		driver2.prime().init();
		Long taskId = driver1.sendNotification(taskName, "test10");
		driver1.getContext().notificationManager.processNotifications();
		sleepMs(100);
		driver2.getContext().notificationManager.processNotifications();

		TaskRec taskRec = tedDao.getTask(taskId);
		TestUtils.print(taskRec.toString());
		assertEquals("NEW", taskRec.status);
		dao_execSql("update tedtask set nextts = nextts - interval '3 seconds' where taskid = " + taskId);
		sleepMs(50);
		driver1.getContext().notificationManager.processNotifications();
		driver2.getContext().notificationManager.processNotifications();
		sleepMs(50);
		taskRec = tedDao.getTask(taskId);
		TestUtils.print(taskRec.toString());
		assertEquals("DONE", taskRec.status);
		assertEquals("1st instance called", 1, calls[0]);
		assertEquals("2st instance called", 1, calls[1]);
	}


}
