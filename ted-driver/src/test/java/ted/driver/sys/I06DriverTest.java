package ted.driver.sys;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedProcessor;
import ted.driver.Ted.TedRetryScheduler;
import ted.driver.Ted.TedStatus;
import ted.driver.TedDriverApi.TedDriverConfig;
import ted.driver.TedResult;
import ted.driver.TedTask;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.TedDriverImpl.TedContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author Augustus
 *         created on 2016.09.19
 */
public class I06DriverTest extends TestBase {
    private final static Logger logger = LoggerFactory.getLogger(I06DriverTest.class);

    private TedDriverImpl driver;
    private TedContext context;

    @Override
    protected TedDriverImpl getDriver() { return driver; }

    @Before
    public void init() throws IOException {
        Properties properties = TestUtils.readPropertiesFile("ted-I06.properties");
        this.driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID, properties);
        this.context = driver.getContext();
    }



    public static class Test06ProcessorLongOk implements TedProcessor {
        @Override
        public TedResult process(TedTask task)  {
            logger.info(this.getClass().getSimpleName() + " process. sleep for 1000ms");
            try {
                TestUtils.sleepMs(1000);
            } catch (Exception e) {
                //e.printStackTrace();
                return TedResult.error("Interrupted");
            }
            return TedResult.done();
        }
    }

    public static class Test06ProcessorLongIgnoreInterrupt implements TedProcessor {
        @Override
        public TedResult process(TedTask task)  {
            logger.info(this.getClass().getSimpleName() + " process. sleep for 1000ms");
            try {
                TestUtils.sleepMs(1000);
            } catch (Exception e) {
                TestUtils.sleepMs(1000); // ignore..
            }
            return TedResult.done();
        }
    }

    public static class Test06ProcessorFastOk implements TedProcessor {
        @Override
        public TedResult process(TedTask task)  {
            logger.info(this.getClass().getSimpleName() + " process.");
            TestUtils.sleepMs(10);
            return TedResult.done();
        }
    }

    private static class ChkTaskStatus {
        boolean isAnyNew = false;
        boolean isAnyInterrupted = false;
        boolean isAnyReturnedUnfinished = false;
        void checkTask(TaskRec taskRec) {
            if (taskRec.status.equals("NEW"))
                isAnyNew = true;
            else if (taskRec.status.equals("ERROR") && taskRec.msg.equals("Interrupted"))
                isAnyInterrupted = true;
            else if (taskRec.status.equals("RETRY") && taskRec.msg.equals("Too long in status [work] (stopped on shutdown)"))
                isAnyReturnedUnfinished = true;
        }
    }

    @Test
    public void test01ShutdownWOInterrupt() {
        String taskName = "TEST06-01";
        dao_cleanupAllTasks();

        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(Test06ProcessorLongIgnoreInterrupt.class), 1, null, Model.CHANNEL_MAIN);

        Long taskId1 = driver.createTask(taskName, null, null, null);
        Long taskId2 = driver.createTask(taskName, null, null, null);

        driver.start();
        TestUtils.sleepMs(300);
        // 1 task is processing and other is waiting in queue. last one should be returned to status 'NEW'
        TestUtils.print("Start to shutdown");
        driver.shutdown(200);
        TestUtils.print("Shutdown finished");

        ChkTaskStatus chk = new ChkTaskStatus();
        chk.checkTask(driver.getContext().tedDao.getTask(taskId1));
        chk.checkTask(driver.getContext().tedDao.getTask(taskId2));

        assertTrue("One of task is in status NEW?", chk.isAnyNew);
        //assertTrue("One of task is in status ERROR (interrupted)?", isInterrupted);
        assertTrue("One task is in RETRY afterTimeout?", chk.isAnyReturnedUnfinished);

        TestUtils.sleepMs(100);
        TestUtils.print("finish");
    }

    @Test
    public void test01ShutdownWithInterrupt() {
        String taskName = "TEST06-01";
        dao_cleanupAllTasks();

        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(Test06ProcessorLongOk.class), 1, null, Model.CHANNEL_MAIN);

        Long taskId1 = driver.createTask(taskName, null, null, null);
        Long taskId2 = driver.createTask(taskName, null, null, null);

        driver.start();
        TestUtils.sleepMs(300);
        // 1 task is processing and other is waiting in queue. last one should be returned to status 'NEW'
        TestUtils.print("Start to shutdown");
        driver.shutdown(200);
        TestUtils.print("Shutdown finished");

        ChkTaskStatus chk = new ChkTaskStatus();
        chk.checkTask(driver.getContext().tedDao.getTask(taskId1));
        chk.checkTask(driver.getContext().tedDao.getTask(taskId2));

        assertTrue("One of task is in status NEW?", chk.isAnyNew);
        assertTrue("One of task is in status ERROR (interrupted)?", chk.isAnyInterrupted);
        //assertTrue("One task is in RETRY afterTimeout?", chk.isAnyReturnedUnfinished);

        TestUtils.sleepMs(100);
        TestUtils.print("finish");
    }


    @Test
    public void test01Shutdown3() {
        String taskName = "TEST06-01";
        dao_cleanupAllTasks();

        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(Test06ProcessorFastOk.class), 1, null, Model.CHANNEL_MAIN);

        driver.createTask(taskName, null, null, null);
        driver.createTask(taskName, null, null, null);

        driver.start();
        TestUtils.sleepMs(50);
        driver.shutdown(10); // no working tasks left after 50ms
        TestUtils.print("finish test");

    }

    @Ignore
    @Test
    public void test05Reject() {
        String taskName = "TEST06-01";
        dao_cleanupAllTasks();

        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(Test06ProcessorLongOk.class), 1, null, Model.CHANNEL_MAIN);
        // create more tasks, than allowed in queue
        List<TaskRec> dummyTasks = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            TaskRec taskRec = new TaskRec();
            taskRec.taskId = -1000L - i;
            taskRec.name = taskName;
            taskRec.channel = "MAIN";
            taskRec.system = TestConfig.SYSTEM_ID;
            taskRec.status = TedStatus.WORK.toString();
            dummyTasks.add(taskRec);
        }
        driver.getContext().taskManager.sendTaskListToChannels(dummyTasks);
        TestUtils.sleepMs(1000);
        driver.shutdown(10); // no working tasks left after 50ms
        TestUtils.print("finish test");

    }

    @Test
    public void test06GetDriverConfig() {

        TedRetryScheduler fakeRetryScheduler = new TedRetryScheduler() {
            @Override
            public Date getNextRetryTime(TedTask task, int retryNumber, Date startTime) {
                return null;
            }
        };
        driver.registerTaskConfig("TASK6", TestTedProcessors.forClass(Test06ProcessorLongIgnoreInterrupt.class), 1, fakeRetryScheduler, Model.CHANNEL_MAIN);

        TedDriverConfig config = driver.getTedDriverConfig();

        assertSame(fakeRetryScheduler, config.getTaskConfig("TASK6").getRetryScheduler());

        assertNull(config.getTaskConfig("X"));

    }

}
