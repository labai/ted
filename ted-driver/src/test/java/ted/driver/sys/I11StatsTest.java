package ted.driver.sys;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedStatus;
import ted.driver.sys.Trash.TedMetricsEvents;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.TedDriverImpl.TedContext;
import ted.driver.sys.TestTedProcessors.TestProcessorOk;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static ted.driver.sys.TestUtils.print;
import static ted.driver.sys.TestUtils.sleepMs;

public class I11StatsTest extends TestBase {
	private final static Logger logger = LoggerFactory.getLogger(I01SimpleTest.class);

	private TedDriverImpl driver;
	private TedContext context;

	@Override
	protected TedDriverImpl getDriver() { return driver; }

	@Before
	public void init() throws IOException {
		Properties properties = TestUtils.readPropertiesFile("ted-I01.properties");
		driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID, properties);
		this.context = driver.getContext();

	}

	@Test
	public void testStats() {
		String taskName = "TEST01-02";
		TestMetricsRegistry metrics = new TestMetricsRegistry();
		driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));
		driver.setMetricsRegistry(metrics);

		Long taskId = driver.createTask(taskName, null, null, null);

		TaskRec taskRec = driver.getContext().tedDao.getTask(taskId);
		print(taskRec.toString());
		assertEquals("NEW", taskRec.status);

		// will start parallel
		driver.getContext().taskManager.processChannelTasks();

		taskRec = driver.getContext().tedDao.getTask(taskId);
		print(taskRec.toString());
		assertEquals("WORK", taskRec.status);

		sleepMs(500);

		// here we wait for time, not finish event..
		taskRec = driver.getContext().tedDao.getTask(taskId);
		assertEquals("DONE", taskRec.status);

		assertTrue(metrics.dbCallCount > 0);
		assertTrue(metrics.loadTaskCount > 0);
		assertTrue(metrics.startTaskCount > 0);
		assertTrue(metrics.finishTaskCount > 0);

	}


	private class TestMetricsRegistry implements TedMetricsEvents {
		int dbCallCount = 0;
		int startTaskCount = 0;
		int finishTaskCount = 0;
		int loadTaskCount = 0;

		@Override
		public void dbCall(String logId, int resultCount, int durationMs) {
			dbCallCount++;
			logger.info("METRICS dbcall {} results={} time={}ms", logId, resultCount, durationMs);
		}

		@Override
		public void startTask(long taskId, String taskName, String channel) {
			startTaskCount++;
			logger.info("METRICS startTask {} {} {}", taskName, taskId, channel);
		}

		@Override
		public void finishTask(long taskId, String taskName, String channel, TedStatus status, int durationMs) {
			finishTaskCount++;
			logger.info("METRICS finishTask {} {} {} status={} time={}", taskName, taskId, channel, status, durationMs);
		}

		@Override
		public void loadTask(long taskId, String taskName, String channel) {
			loadTaskCount++;
			logger.info("METRICS loadTask {} {} {}", taskName, taskId, channel);
		}
	}
}
