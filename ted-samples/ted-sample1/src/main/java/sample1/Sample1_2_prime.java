package sample1;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedDbType;
import ted.driver.TedDriver;
import ted.driver.TedResult;
import ted.driver.TedTask;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Augustus
 *         created on 2018.08.01
 */
public class Sample1_2_prime {
	private static final Logger logger = LoggerFactory.getLogger(Sample1_2_prime.class);

	private static final String TASK_NAME = "PROCESS_LINE";

	// connection to db configuration
	//
	private static DataSource dataSource() {
		HikariDataSource dataSource = new HikariDataSource();
		dataSource.setDriverClassName("org.postgresql.Driver");
		dataSource.setJdbcUrl("jdbc:postgresql://localhost:5433/ted");
		dataSource.setUsername("ted");
		dataSource.setPassword("ted");
		return dataSource;
	}

	private static TedDriver tedDriver() {
		Properties properties = new Properties();
		String propFileName = "ted.properties";
		InputStream inputStream = Sample1_2_prime.class.getClassLoader().getResourceAsStream(propFileName);
		if (inputStream == null)
			throw new RuntimeException("Property file '" + propFileName + "' not found in the classpath");
		try {
			properties.load(inputStream);
			inputStream.close();
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
		TedDriver tedDriver1 = tedDriver();
		tedDriver1.registerTaskConfig(TASK_NAME, s -> Sample1_2_prime::processLine) ;
		tedDriver1.enablePrime();
		tedDriver1.setOnBecomePrimeHandler(() -> logger.info("tedDriver1 BECOME PRIME"));
		tedDriver1.setOnLostPrimeHandler(() -> logger.info("tedDriver1 LOST PRIME"));
		tedDriver1.start();


		TedDriver tedDriver2 = tedDriver();
		tedDriver2.registerTaskConfig(TASK_NAME, s -> Sample1_2_prime::processLine) ;
		tedDriver2.enablePrime();
		tedDriver2.setOnBecomePrimeHandler(() -> logger.info("tedDriver2 BECOME PRIME"));
		tedDriver2.setOnLostPrimeHandler(() -> logger.info("tedDriver2 LOST PRIME"));
		tedDriver2.start();

		// read some big file for processing
		//
		File file = new File(Sample1_2_prime.class.getClassLoader().getResource(fileName).getPath());
		List<String> lines = FileUtils.readLines(file, "UTF-8");

		// create tasks for each line
		//
		for (String line : lines) {
			tedDriver1.createTask(TASK_NAME, line);
		}

		sleep(1000);
		tedDriver1.shutdown();
		// wait a while, while ted will process tasks. see processing info in logs
		//
		sleep(7000);

		//tedDriver1.shutdown();
		tedDriver2.shutdown();
		System.out.println("finish sample1_2_prime");
	}

	// file line processor
	//
	private static TedResult processLine(TedTask task) {
		if (isEmpty(task.getData()))
			return TedResult.error("task.data is empty");
		int sleepMs = RandomUtils.nextInt(200, 900);
		System.out.println("PROCESS LINE " + task.getData());
		logger.info("do something smart with line: '{}' for {}ms", task.getData(), sleepMs);
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
