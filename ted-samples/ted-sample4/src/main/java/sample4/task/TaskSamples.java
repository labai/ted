package sample4.task;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ted.driver.TedTask;
import ted.spring.annotation.TedSchedulerProcessor;
import ted.spring.annotation.TedTaskProcessor;

import java.util.Random;

/**
 * @author Augustus
 *         created on 2019.12.04
 */
@Component
public class TaskSamples {
    private static final Logger logger = LoggerFactory.getLogger(TaskSamples.class);

    @Autowired
    private Gson gson;

    private Random random = new Random();

    public static class TaskParam {
        public int lineNumber;
        public String line;
    }

    // task may return String on success - it will be saved into tedtask.msg
    @TedTaskProcessor(name = "TASK3")
    public String task3(TedTask task) {
        logger.info("Start TASK3");
        TaskParam data = gson.fromJson(task.getData(), TaskParam.class);
        int sleepMs = 200 + random.nextInt(700);
        logger.info("do something smart with line {}: '{}' for {}ms", data.lineNumber, data.line, sleepMs);
        sleep(sleepMs);
        return "Task done";
    }


    // scheduler task
    @TedSchedulerProcessor(name = "SCH_1", cron = "1 * * * * *")
    public String schedulerTask1() {
        logger.info("Start schedulerTask1");
        return "ok";
    }


    private static void sleep(long milis) {
        try {
            Thread.sleep(milis);
        } catch (InterruptedException e2) {
        }
    }

}
