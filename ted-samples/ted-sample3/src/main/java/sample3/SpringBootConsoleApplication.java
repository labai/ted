package sample3;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import sample3.configuration.TedConfig;
import ted.driver.TedDriver;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static java.lang.Thread.sleep;

/**
 * @author Augustus
 *         created on 2018.08.25
 */
@SpringBootApplication
public class SpringBootConsoleApplication implements CommandLineRunner {
	private static final Logger logger = LoggerFactory.getLogger(SpringBootConsoleApplication.class);

	@Autowired
	private TedDriver tedDriver;

	public static void main(String[] args) {
		logger.info("Starting TedSample3");
		SpringApplication
				.run(SpringBootConsoleApplication.class, args)
				.close();
		logger.info("Finish TedSample3");
	}

	@Override
	public void run(String... args) throws Exception {
		logger.info("Create few tasks and wait for execution");
		createTasks();
		sleep(5000);
	}

	private void createTasks() throws IOException {
		String fileName = "input.txt";
		File file = new File(SpringBootConsoleApplication.class.getClassLoader().getResource(fileName).getPath());
		List<String> lines = FileUtils.readLines(file, "UTF-8");

		// create tasks for each line
		//
		for (String line : lines) {
			tedDriver.createTask(TedConfig.SAMPLE_TASK_NAME, line);
		}


	}
}
