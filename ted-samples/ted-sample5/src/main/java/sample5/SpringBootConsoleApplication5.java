package sample5;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;
import ted.driver.task.TedTaskFactory;

import java.io.IOException;
import java.util.List;

import static java.lang.Thread.sleep;

/**
 * @author Augustus
 *         created on 2019.12.04
 */
@SpringBootApplication
public class SpringBootConsoleApplication5 implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(SpringBootConsoleApplication5.class);

    @Autowired
    private TedTaskFactory tedTaskFactory;

    public static void main(String[] args) {
        logger.info("Starting TedSample4");
        SpringApplication
            .run(SpringBootConsoleApplication5.class, args)
            .close();
        logger.info("Finish TedSample4");
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("Create few tasks and wait for execution");
        createTasks();
        sleep(10000);
    }

    private void createTasks() throws IOException {
        String fileName = "input.txt";
        ClassPathResource dataFile = new ClassPathResource(fileName);
        List<String> lines = FileUtils.readLines(dataFile.getFile(), "UTF-8");

        // create task for each line
        int inum = 0;
        for (String line : lines) {
            tedTaskFactory.createTask("TASK1", line);
        }
    }

}
