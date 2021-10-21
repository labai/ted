package ted.driver.sys;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedProcessor;
import ted.driver.TedResult;
import ted.driver.TedTask;
import ted.driver.sys.JdbcSelectTed.JetJdbcParamType;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.QuickCheck.Tick;
import ted.driver.sys.TedDriverImpl.TedContext;
import ted.driver.sys.TestTedProcessors.TestProcessorOk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static ted.driver.sys.JdbcSelectTed.sqlParam;
import static ted.driver.sys.MiscUtils.asList;
import static ted.driver.sys.TestUtils.awaitProcessUntilStatus;
import static ted.driver.sys.TestUtils.awaitUntilTaskFinish;
import static ted.driver.sys.TestUtils.print;
import static ted.driver.sys.TestUtils.sleepMs;

/**
 * @author Augustus
 *         created on 2017.09.15
 *
 *  creates a lot of tasks.
 */
// @Ignore("batches are not used yet")
public class I07BatchTest extends TestBase {
    private final static Logger logger = LoggerFactory.getLogger(I07BatchTest.class);

    private TedDriverImpl driver;
    private TedDao tedDao;
    private TedContext context;

    @Override
    protected TedDriverImpl getDriver() { return driver; }

    @Before
    public void init() throws IOException {
        Properties properties = TestUtils.readPropertiesFile("ted-I07.properties");
        this.driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID, properties);
        this.tedDao = driver.getContext().tedDao;
        this.context = driver.getContext();
    }

    @Ignore
    @Test
    public void testCreate200() {
        String taskName = "TEST01-01";
        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));
        ((TedDaoAbstract)context.tedDao).getSequenceNextValue("SEQ_TEDTASK_BNO");

        long startTs = System.currentTimeMillis();
        for (int i = 0; i < 2000; i++) {
            driver.createTask(taskName, "test i07", "test1", "" + i);
        }
        // 200 items - 160, 182, 181
        // 2k - 1215, 898, 993
        TestUtils.log("create 200 in {0}s", System.currentTimeMillis() - startTs);

    }

    @Ignore
    @Test
    public void testCreateBulk200() {
        String taskName = "TEST01-01";

        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));
        ((TedDaoAbstract)context.tedDao).getSequenceNextValue("SEQ_TEDTASK_BNO");
        String param = "x";
        // for (int i = 0; i < 1000; i++) {
        // 	param += i + " ---------------------------------------------------------------------------------------------------- " + i + "\n";
        // }

        long startTs = System.currentTimeMillis();
        List<TedTask> taskParams = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            taskParams.add(driver.newTedTask(taskName, "te\tst\\. i07" + param, "\\N", null));
        }
        driver.createTasksBulk(taskParams, null, null);
        // postgres.copy 200 - 35, 39, 41
        //    2k - 100, 102, 117
        TestUtils.log("create 200 in {0}s", System.currentTimeMillis() - startTs);

    }



    // create batch tasks, wait for finish (one of subtasks will retry once).
    // after subtasks finish, batch task processor will be executed and will return retry itself.
    @Test
    public void testBatch1() {
        dao_cleanupAllTasks();

        String taskName = "TEST07-1";
        String batchName = "BAT07";
        String batchChannel = "BAT";

        logger.info("###STEP 1. Start - init...");
        driver.registerTaskConfig(taskName, s -> new ProcessorRandomOk());
        driver.registerTaskConfig(batchName, s -> new BatchFinishProcessor());

        // create tasks
        logger.info("###STEP 2. Create tasks...");
        List<TedTask> taskParams = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            taskParams.add(driver.newTedTask(taskName, ""+i, null, null));
        }
        final Long batchId = driver.createBatch(batchName, "data", "key1", "key2", taskParams, null);

        final List<TaskRec> tasks = getBatchTasks(batchId);

        // wait a bit (not all tasks are finished yet)
        logger.info("###STEP 3. Not all tasks are finished...");
        setTaskNextTsNow(batchId); // there can be difference between clocks in dev/tomcat and db server?..
        sleepMs(10);
        context.taskManager.processChannelTasks(new Tick(1));
        context.batchWaitManager.processBatchWaitTasks();

        awaitUntilTaskFinish(driver, batchId, 300);

        TaskRec batchRec = tedDao.getTask(batchId);
        print(batchRec.toString());
        print(batchRec.getTedTask().toString());
        assertEquals("batch should be RETRY until all task will be finished", "RETRY", batchRec.status);
        assertEquals("Batch task is waiting for finish of subtasks", batchRec.msg);
        print("sleep...");

        // wait till all tasks are finished
        logger.info("###STEP 4. All tasks are finished...");
        await().atMost(2600, TimeUnit.MILLISECONDS).pollInterval(TestUtils.POLL_INTERVAL).until(() -> {
            context.taskManager.flushStatuses();
            context.taskManager.processChannelTasks(new Tick(1)); // here all subtask should be finished
            List<String> finished = asList("DONE", "ERROR");
            for (TaskRec rec : tasks) {
                TaskRec rec2 = context.tedDao.getTask(rec.taskId);
                logger.debug("task {} status {}", rec2.taskId, rec2.status);
                if (! finished.contains(rec2.status))
                    return false;
            }
            return true;
        });

        // here batch task may be changed from RETRY to NEW, and then can be processed and again changed
        logger.info("###STEP 5. Batch task converted to 'normal'...");
        TaskRec batchTask = context.tedDao.getTask(batchId);
        await().atMost(300, TimeUnit.MILLISECONDS).pollInterval(TestUtils.POLL_INTERVAL).until(() -> {
            context.batchWaitManager.processBatchWaitTask(batchTask);
            TaskRec rec = tedDao.getTask(batchId);
            return !Model.BATCH_MSG.equals(rec.msg); // wait till start batch
        });

        logger.info("###STEP 6. Check batch task status (batch retry test)...");
        // wait till exec batch (now it is same as regular task), first execution fails to retry (see BatchFinishProcessor)
        awaitProcessUntilStatus(driver, batchId, asList("RETRY", "DONE", "ERROR"), 500);

        batchRec = tedDao.getTask(batchId);
        print(batchRec.toString());
        assertEquals("batch should be RETRY because 1 task was delayed and not finished yet", "RETRY", batchRec.status);
        assertEquals("channel should be as is in config", batchChannel, batchRec.channel);

        logger.info("###STEP 7. Subtasks finished, waiting for batch tasks own retry...");
        awaitProcessUntilStatus(driver, batchId, asList("DONE", "ERROR"), 2600);

        batchRec = tedDao.getTask(batchId);
        print(batchRec.toString());
        assertEquals("batch should be finished", "DONE", batchRec.status);
        assertEquals("retries should be cleanup after batch subtasks finished", 1L, batchRec.retries.longValue());

        logger.info("###STEP 8. Finished");
        print("batchTaskRec: " + batchRec);
    }

    // set second task to retry
    static class ProcessorRandomOk implements TedProcessor {
        @Override
        public TedResult process(TedTask task)  {
            logger.info(this.getClass().getSimpleName() + " process " + task.getData());
            sleepMs(20);
            if ("1".equals(task.getData())) {
                if (task.getRetries() == 0) {
                    logger.info("Set task {} to RETRY", task.getTaskId());
                    return TedResult.retry();
                } else { // next attempts
                    logger.info("Set task {} to DONE", task.getTaskId());
                    return TedResult.done();
                }
            }
            if ("2".equals(task.getData())) {
                logger.info("Set task {} to ERROR", task.getTaskId());
                return TedResult.error();
            }
            return TedResult.done();
        }
    }

    static class BatchFinishProcessor implements TedProcessor {
        @Override
        public TedResult process(TedTask task)  {
            logger.info(this.getClass().getSimpleName() + " process " + task.getRetries());
            //sleepMs(20);
            if (task.getRetries() == 0)
                return TedResult.retry("retry batch finish task");
            else
                return TedResult.done();
        }
    }

    private void setTaskNextTsNow(Long taskId) {
        String sql = "update tedtask set nextts = $now where taskId=" + taskId
            + " and status in ('NEW', 'RETRY')";
        sql = sql.replace("$now", tedDao.getDbType().sql().now());
        ((TedDaoAbstract)tedDao).execute("setTaskNextTsNow", sql, Collections.emptyList());
    }


    private List<TaskRec> getBatchTasks(Long batchId) {
        String sql = "select * from tedtask where batchid = ?";
        List<TaskRec> results = ((TedDaoAbstract)tedDao).selectData("batchIds", sql, TaskRec.class, asList(
            sqlParam(batchId, JetJdbcParamType.LONG)
        ));
        return results;
    }

}
