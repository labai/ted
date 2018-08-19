package sample1;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import labai.ted.Ted.TedDbType;
import labai.ted.Ted.TedProcessor;
import labai.ted.TedDriver;
import labai.ted.TedResult;
import labai.ted.TedTask;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.isEmpty;

public class Sample1_4_batch {
	private static final Logger logger = LoggerFactory.getLogger(Sample1_4_batch.class);

	private static final String TASK_NAME = "PROCESS_LINE";
	private static final String BATCH_TASK = "FINISH_PROCESS";

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
		InputStream inputStream = Sample1_4_batch.class.getClassLoader().getResourceAsStream(propFileName);
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
		tedDriver.registerTaskConfig(TASK_NAME, taskName -> lineTaskProcessor()) ;
		tedDriver.registerTaskConfig(BATCH_TASK, taskName -> batchTaskProcessor()) ;
		tedDriver.start();

		// read some big file for processing
		//
		File file = new File(Sample1_4_batch.class.getClassLoader().getResource(fileName).getPath());
		List<String> lines = FileUtils.readLines(file, "UTF-8");

		// create tasks for each line
		//
		List<TedTask> tasks = new ArrayList<>();
		for (String line : lines) {
			tasks.add(TedDriver.newTedTask(TASK_NAME, line, null, null));
		}
		Long batchId = tedDriver.createBatch(BATCH_TASK, "batch task data", null, null, tasks);

		// wait a while, while ted will process tasks. see processing info in logs
		//
		sleep(10000);

		tedDriver.shutdown();
		System.out.println("finish sample1_4_batch");
	}

	// file line processor
	//
	private static TedProcessor lineTaskProcessor() {
		return task -> {
			if (isEmpty(task.getData()))
				return TedResult.error("task.data is empty");
			int sleepMs = RandomUtils.nextInt(200, 900);
			logger.info("do something smart with line: '{}' for {}ms", task.getData(), sleepMs);
			sleep(sleepMs);
			return TedResult.done();
		};
	}

	//
	//
	private static TedProcessor batchTaskProcessor() {
		return task -> {
			logger.info("all tasks finished");
			return TedResult.done();
		};
	}

	private static void sleep(long milis) {
		try {
			Thread.sleep(milis);
		} catch (InterruptedException e2) {
		}
	}

}