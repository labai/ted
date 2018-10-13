package sample1;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedDbType;
import ted.driver.TedDriver;
import ted.driver.TedResult;
import ted.driver.TedTask;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Augustus
 *         created on 2018.08.01
 */
public class Sample1_3_events {
	private static final Logger logger = LoggerFactory.getLogger(Sample1_3_events.class);

	private static final String TASK_NAME = "PROCESS_LINE";

	// connection to db configuration
	//
	private static DataSource dataSource() {
		ComboPooledDataSource dataSource = new ComboPooledDataSource();
		try {
			dataSource.setDriverClass("org.postgresql.Driver");
			dataSource.setJdbcUrl("jdbc:postgresql://localhost:5433/ted");
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
		InputStream inputStream = Sample1_3_events.class.getClassLoader().getResourceAsStream(propFileName);
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
		TedDriver tedDriver = tedDriver();
		tedDriver.registerTaskConfig(TASK_NAME, s -> Sample1_3_events::processLine) ;
		tedDriver.start();

		// read data for processing
		//
		File file = new File(Sample1_3_events.class.getClassLoader().getResource(fileName).getPath());
		List<String> lines = FileUtils.readLines(file, "UTF-8");

		// create tasks for each line
		//
		System.out.println("create events for 'obj-1001");
		int inum = 0;

		for (String line : lines) {
			tedDriver.createEvent(TASK_NAME, "obj-1001", line, line + "-" + inum++);
		}
		System.out.println("create events for 'obj-1002");
		inum = 0;
		Collections.reverse(lines);
		for (String line : lines) {
			tedDriver.createEvent(TASK_NAME, "obj-1002", line, line + "-" + inum++);
		}

		// wait a while, while ted will process tasks. see processing info in logs
		// events should be executed in order they were created
		//
		sleep(6000);

		tedDriver.shutdown();
		System.out.println("finish sample1_3_events");
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
