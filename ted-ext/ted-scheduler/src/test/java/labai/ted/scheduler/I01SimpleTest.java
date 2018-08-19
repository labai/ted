package labai.ted.scheduler;

import labai.ted.TedDriver;
import labai.ted.scheduler.utils.CronExpression;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Properties;

public class I01SimpleTest {
	private final static Logger logger = LoggerFactory.getLogger(I01SimpleTest.class);

	private TedDriver driver;
	//private TedContext context;
	private TedScheduler scheduler;


	@Before
	public void init() throws IOException {
		Properties properties = TestUtils.readPropertiesFile("ted-I01.properties");
		driver = new TedDriver(TestConfig.testDbType, TestConfig.getDataSource(), properties);
		driver.enablePrime();
		driver.start();
		scheduler = new TedScheduler(driver);
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
					logger.info("eina 1");
				}).register();
		TestUtils.sleepMs(2000);
	}

	public static void print(String msg){
		System.out.println(msg);
	}
}
