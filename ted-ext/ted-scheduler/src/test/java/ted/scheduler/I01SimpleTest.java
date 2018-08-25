package ted.scheduler;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedStatus;
import ted.driver.TedDriver;
import ted.driver.TedTask;
import ted.scheduler.TedSchedulerImpl.CronRetry;
import ted.scheduler.TedSchedulerImpl.Factory;
import ted.scheduler.utils.CronExpression;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static ted.scheduler.TestUtils.sleepMs;

public class I01SimpleTest {
	private final static Logger logger = LoggerFactory.getLogger(I01SimpleTest.class);

	private TedDriver driver;
	private TedScheduler scheduler;
	private TedSchedulerImpl schedulerImpl;
	private DaoPostgres dao;


	@Before
	public void init() throws IOException {
		Properties properties = TestUtils.readPropertiesFile("ted-I01.properties");
		driver = new TedDriver(TestConfig.testDbType, TestConfig.getDataSource(), properties);
		dao = new DaoPostgres(TestConfig.getDataSource(), properties.getProperty("ted.systemId"));
		driver.enablePrime();
		driver.start();
		scheduler = new TedScheduler(driver);
		schedulerImpl = new TedSchedulerImpl(driver);
		//this.context = driver.getContext();

	}

	@Test
	public void test1() {
		CronExpression cron = new CronExpression("0/11 14/19 0 1,11,21 1/2 *");
		ZonedDateTime zdt = cron.nextTimeAfter(ZonedDateTime.now());
		print((zdt = cron.nextTimeAfter(zdt)).toString());
		print((zdt = cron.nextTimeAfter(zdt)).toString());
		print((zdt = cron.nextTimeAfter(zdt)).toString());
	}

	@Test
	public void testSchd01() {
		scheduler.builder().name("TEST1")
				.scheduleCron("0 0/10 * 1/1 * *")
				.runnable(() -> {
					logger.info("executing scheduler task");
				}).register();
		sleepMs(2000);
	}

	@Test
	public void testMaint() {
		Long taskId = schedulerImpl.registerScheduler("TEST1", null,
				Factory.single(() -> logger.info("executing scheduler task")),
				new CronRetry("0 0/10 * ? * *"));
		print("scheduler taskId=" + taskId);
		TedTask task = driver.getTask(taskId);
		assertEquals(TedStatus.RETRY, task.getStatus());
		sleepMs(20);
		dao_execSql("update tedtask set status = 'ERROR', nextts = null where taskid = " + taskId + " returning taskid");
		sleepMs(20);
		task = driver.getTask(taskId);
		assertEquals(TedStatus.ERROR, task.getStatus());
		schedulerImpl.checkForErrorStatus();
		sleepMs(20);
		task = driver.getTask(taskId);
		assertEquals(TedStatus.RETRY, task.getStatus());
		sleepMs(2000);
	}


	public static void print(String msg){
		System.out.println(msg);
	}

	private void dao_execSql (String sql) {
		dao.selectData("test", sql, Void.class, Collections.emptyList());
	}

}
