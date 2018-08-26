package sample1;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedDbType;
import ted.driver.TedDriver;
import ted.driver.TedResult;
import ted.driver.TedTask;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Augustus
 *         created on 2018.08.01
 */
public class Sample1_5_notify {
	private static final Logger logger = LoggerFactory.getLogger(Sample1_5_notify.class);

	private static final String TASK_NAME = "NTF_SAMPLE";

	// connection to db configuration
	//
	private static DataSource dataSource() {
		ComboPooledDataSource dataSource = new ComboPooledDataSource();
		try {
			dataSource.setDriverClass("org.postgresql.Driver");
			dataSource.setJdbcUrl("jdbc:postgresql://localhost:5433/ted");
//			dataSource.setDriverClass("oracle.jdbc.OracleDriver");
//			dataSource.setJdbcUrl("jdbc:oracle:thin:@localhost:1521:XE");
			dataSource.setUser("ted");
			dataSource.setPassword("ted");
		} catch (PropertyVetoException e) {
			throw new RuntimeException(e);
		}
		return dataSource;
	}

	private static TedDriver tedDriver() {
		Properties properties = new Properties();
		String propFileName = "ted.properties";
		InputStream inputStream = Sample1_5_notify.class.getClassLoader().getResourceAsStream(propFileName);
		if (inputStream == null)
			throw new RuntimeException("Property file '" + propFileName + "' not found in the classpath");
		try {
			properties.load(inputStream);
		} catch (IOException e) {
			throw new RuntimeException("Cannot read property file '" + propFileName + "'", e);
		}
		DataSource dataSource = dataSource();
		TedDriver tedDriver = new TedDriver(TedDbType.POSTGRES, dataSource, properties);
		return tedDriver;


	}

	public static void main(String ... args) throws IOException {
		String fileName = "input.txt";
		System.out.println("start sample1");

		// init ted, register tasks
		//
		TedDriver tedDriver = tedDriver();
		tedDriver.registerTaskConfig(TASK_NAME, s -> Sample1_5_notify::processNotification) ;
		tedDriver.start();

		TedDriver tedDriver2 = tedDriver();
		tedDriver2.registerTaskConfig(TASK_NAME, s -> Sample1_5_notify::processNotification) ;
		tedDriver2.start();
		// send notifications to all active instances
		for (int i = 0; i < 5; i++) {
			tedDriver.sendNotification(TASK_NAME, "notify:x" + i);
			sleep(1000);
		}
		sleep(500);
		tedDriver.shutdown();
		tedDriver2.shutdown();
		System.out.println("finish sample1_5_notify");
	}

	// file line processor
	//
	private static TedResult processNotification(TedTask task) {
		int sleepMs = RandomUtils.nextInt(5, 100);
		System.out.println("NOTIFY " + task.getData() + " - " + Thread.currentThread().getName());
		logger.info("notify {}, {}ms", task.getData(), sleepMs);
		sleep(sleepMs);
		return TedResult.done();
	}

	private static void sleep(long milis) {
		try {
			Thread.sleep(milis);
		} catch (InterruptedException e2) {
		}
	}

}
