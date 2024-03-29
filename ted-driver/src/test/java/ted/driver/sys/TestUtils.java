package ted.driver.sys;

import org.awaitility.Awaitility;
import org.awaitility.Duration;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.QuickCheck.Tick;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

/**
 * @author Augustus
 *         created on 2016.09.20
 */
class TestUtils {
    static final Duration POLL_INTERVAL = new Duration(30, TimeUnit.MILLISECONDS);

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("mm:ss.SSS");

    static Properties readPropertiesFile(String propFileName) throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = TestBase.class.getClassLoader().getResourceAsStream(propFileName);
        if (inputStream == null)
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        properties.load(inputStream);
        return properties;
    }

    static void sleepMs(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException("Can't sleep", e);
        }
    }


    public static void log(String msg){
        System.out.println(dateFormat.format(new Date()) + " " + msg);
    }

    public static void log(String pattern, Object ... args){
        String msg = MessageFormat.format(pattern, args);
        log(msg);
    }

    public static void print(String msg){
        System.out.println(msg);
    }


//	public static void printJson(Object object){
//		System.out.println(gson.toJson(object));
//	}

    public static String shortTime(Date date) {
        return dateFormat.format(date);
    }

    public static void awaitTask(int maxMs, Callable<Boolean> conditionEvaluator) {
        Awaitility.await()
            .pollDelay(5, TimeUnit.MILLISECONDS)
            .pollInterval(POLL_INTERVAL)
            .atMost(maxMs, TimeUnit.MILLISECONDS)
            .until(conditionEvaluator);
    }

    // just flush statuses and then get from db
    public static void awaitUntilTaskFinish(final TedDriverImpl driver, final long taskId, int maxMs) {
        awaitTask(maxMs, () -> {
            // driver.getContext().taskManager.processChannelTasks();
            driver.getContext().taskManager.flushStatuses();
            TaskRec rec = driver.getContext().tedDao.getTask(taskId);
            return ! asList("WORK", "NEW").contains(rec.status);
        });
    }

    // just flush statuses and then get from db
    public static void awaitUntilStatus(final TedDriverImpl driver, final long taskId, List<String> statuses, int maxMs) {
        awaitTask(maxMs, () -> {
            driver.getContext().taskManager.flushStatuses();
            TaskRec rec = driver.getContext().tedDao.getTask(taskId);
            return statuses.contains(rec.status);
        });
    }

    // process tasks and wait for status
    public static void awaitProcessUntilStatus(final TedDriverImpl driver, final long taskId, List<String> statuses, int maxMs) {
        awaitTask(maxMs, () -> {
            driver.getContext().taskManager.flushStatuses();
            driver.getContext().taskManager.processChannelTasks(new Tick(1));
            TaskRec rec = driver.getContext().tedDao.getTask(taskId);
            return statuses.contains(rec.status);
        });
    }
}
