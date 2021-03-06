package sample4;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;
import sample4.task.TaskSamples.TaskParam;
import ted.driver.TedTask;
import ted.driver.task.TedTaskFactory;
import ted.spring.annotation.TedTaskProcessor;

import java.io.IOException;
import java.util.List;

import static java.lang.Thread.sleep;

/**
 * @author Augustus
 *         created on 2019.12.04
 */
@SpringBootApplication
public class SpringBootConsoleApplication4 implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(SpringBootConsoleApplication4.class);

    @Autowired
    private Gson gson;

    @Autowired
    private TedTaskFactory tedTaskFactory;

    public static void main(String[] args) {
        logger.info("Starting TedSample4");
        SpringApplication
            .run(SpringBootConsoleApplication4.class, args)
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

        // create few tasks for each line
        //
        int inum = 0;
        for (String line : lines) {
            TaskParam param = new TaskParam();
            param.lineNumber = inum++;
            param.line = line;

            tedTaskFactory.createTask("TASK1", gson.toJson(param));
            tedTaskFactory.createTask("TASK2", gson.toJson(param));
            tedTaskFactory.createTask("TASK3", gson.toJson(param));

            tedTaskFactory.taskBuilder("TASK4")
                .data(gson.toJson(param))
                .key1("key1")
                .create();
        }
    }

    @TedTaskProcessor(name = "TASK4", retryException = { IllegalStateException.class })
    public String task4(TedTask task) {
        logger.info("start TASK4");
        throw new IllegalStateException("should retry");
    }

}
