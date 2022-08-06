package ted.driver.sys;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedDbType;
import ted.driver.sys.Executors.ChannelThreadPoolExecutor;
import ted.driver.sys.Executors.MeasuredRunnable;
import ted.driver.sys.JdbcSelectTed.JetJdbcParamType;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.QuickCheck.Tick;
import ted.driver.sys.TedDriverImpl.TedContext;
import ted.driver.sys.TestTedProcessors.TestProcessorOk;
import ted.driver.sys.TestTedProcessors.TestProcessorOkSleep;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static ted.driver.sys.TestConfig.SYSTEM_ID;
import static ted.driver.sys.TestTedProcessors.forProcessor;
import static ted.driver.sys.TestUtils.print;
import static ted.driver.sys.TestUtils.sleepMs;

/**
 * @author Augustus
 *         created on 2016.09.19
 */
public class I03MaintenanceTest extends TestBase {
    private final static Logger logger = LoggerFactory.getLogger(I03MaintenanceTest.class);

    private TedDriverImpl driver;
    private TedContext context;

    private final String archTable = "tedtaskarch";

    @Override
    protected TedDriverImpl getDriver() { return driver; }

    @Before
    public void init() throws IOException {
        Properties properties = TestUtils.readPropertiesFile("ted-I03.properties");
        this.driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), SYSTEM_ID, properties);
        this.context = driver.getContext();
    }

    private void dao_setDoneAndOld(long taskId, int daysBack) {
        long delta = daysBack * 3600 * 24 * 1000;
        Date longAgo = new Date(System.currentTimeMillis() - delta);
        print("longAgo:" + longAgo);
        ((TedDaoAbstract)context.tedDao).execute("dao_setDoneAndOld",
            " update tedtask set status = 'DONE', createTs = ?, finishTs = ?, nextts = null where taskId = ?", asList(
                JdbcSelectTed.sqlParam(longAgo, JetJdbcParamType.TIMESTAMP),
                JdbcSelectTed.sqlParam(longAgo, JetJdbcParamType.TIMESTAMP),
                JdbcSelectTed.sqlParam(taskId, JetJdbcParamType.LONG)
            ));
    }
    private void dao_setStartTs(long taskId, Date startTs) {
        ((TedDaoAbstract)context.tedDao).execute("dao_setStartTs",
            " update tedtask set startTs = ? where taskId = ?", asList(
                JdbcSelectTed.sqlParam(startTs, JetJdbcParamType.TIMESTAMP),
                JdbcSelectTed.sqlParam(taskId, JetJdbcParamType.LONG)
            ));
    }
    private void dao_setCreateTs(long taskId, Date createTs) {
        ((TedDaoAbstract)context.tedDao).execute("dao_setCreateTs",
            " update tedtask set createTs = ? where taskId = ?", asList(
                JdbcSelectTed.sqlParam(createTs, JetJdbcParamType.TIMESTAMP),
                JdbcSelectTed.sqlParam(taskId, JetJdbcParamType.LONG)
            ));
    }
    private void dao_setNextTs(long taskId, Date nextTs) {
        ((TedDaoAbstract)context.tedDao).execute("dao_setNextTs",
            " update tedtask set nextTs = ? where taskId = ?", asList(
                JdbcSelectTed.sqlParam(nextTs, JetJdbcParamType.TIMESTAMP),
                JdbcSelectTed.sqlParam(taskId, JetJdbcParamType.LONG)
            ));
    }

    private TaskRec dao_getArchTask(long taskId) {
        List<TaskRec> tasks = ((TedDaoAbstract)context.tedDao).selectData("dao_getArchTask",
            " select * from " + archTable + " where taskId = ?", TaskRec.class, asList(
                JdbcSelectTed.sqlParam(taskId, JetJdbcParamType.LONG)
            ));
        return tasks.isEmpty() ? null : tasks.get(0);
    }


    @Test
    public void test01WorkTimeoutMinute() {
        String taskName = "TEST03-01";
        dao_cleanupAllTasks();

        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class), 1, null, Model.CHANNEL_MAIN);
        Long taskId;
        TaskRec taskRec;

        driver.createTask(taskName, null, null, null);
        TestUtils.sleepMs(10);
        // set status to WORK
        List<TaskRec> tasks = context.tedDao.reserveTaskPortion(new HashMap<String, Integer>() {{ put(Model.CHANNEL_MAIN, 1); }}, new Tick(1));
        taskId = tasks.get(0).taskId;

        taskRec = context.tedDao.getTask(taskId);
        //print(taskRec.toString() + " startTs=" + taskRec.startTs);
        assertEquals("WORK", taskRec.status);

        // change startTs to test work timeout
        dao_setStartTs(taskId, new Date(System.currentTimeMillis() - 121 * 1000));
        taskRec = context.tedDao.getTask(taskId);
        //print(taskRec.toString() + " startTs=" + taskRec.startTs);

        context.maintenanceManager.processMaintenanceTasks();

        // task should be canceled due to timeout
        taskRec = context.tedDao.getTask(taskId);
        //print(taskRec.toString());
        assertEquals("RETRY", taskRec.status);
        assertEquals("Too long in status [work](3)", taskRec.msg);

        assertFalse("is not new", taskRec.getTedTask().isNew());
        assertTrue("is retry", taskRec.getTedTask().isRetry());
        assertTrue("is timeout", taskRec.getTedTask().isAfterTimeout());
    }


    // if task TEST03-02 working > 1 min but < 40 min (setup in config), then finishTs should be set to this task.
    @Test
    public void test02WorkTimeoutPostpone() {
        String taskName = "TEST03-02";
        dao_cleanupAllTasks();

        // with timeout 40 minutes
        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));
        //driver.registerTaskConfig(taskName, forClass(Test01ProcessorOk.class), 40, null, "MAIN");
        Long taskId;
        TaskRec taskRec;

        driver.createTask(taskName, null, null, null);

        //driver.getContext().config.defaultTaskTimeoutMn = 30 * 60; // default - 30min

        // set status to work
        List<TaskRec> tasks = context.tedDao.reserveTaskPortion(new HashMap<String, Integer>(){{put("MAIN",1);}}, new Tick(1));
        taskId = tasks.get(0).taskId;

        taskRec = context.tedDao.getTask(taskId);
        //print(taskRec.toString() + " startTs=" + taskRec.startTs);
        assertEquals("WORK", taskRec.status);
        assertNull(taskRec.finishTs); // finish time is not set in beginning

        // 1. 30 min - still working
        dao_setStartTs(taskId, new Date(System.currentTimeMillis() - 31 * 60 * 1000));
        context.maintenanceManager.processMaintenanceTasks();
        taskRec = context.tedDao.getTask(taskId);
        print(taskRec.toString() + " startTs=" + MiscUtils.toTimeString(taskRec.startTs));
        assertEquals("WORK", taskRec.status);
        assertNotNull(taskRec.finishTs); // after processMaintenanceTasks finishTs must be set to startTs + 40 min
        dao_cleanupTasks(taskName);

    }

    // unknown tasks will be postponed for 2 minutes, but after 1 day will be canceled
    @Test
    public void test02UnknownPostpone() throws Exception {
        String taskName = "TEST03-02XX";
        dao_cleanupAllTasks();
        Long taskId;
        TaskRec taskRec;

        // create unknown task
        TedDriverImpl driverTmp = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), SYSTEM_ID, new Properties());
        driverTmp.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class), 40, null, "MAIN");
        taskId = driverTmp.createTask(taskName, null, null, null);

        // process unknown task
        processChannelTasks();
        Thread.sleep(20);

        // 1. Must be postponed (new with some nextts > now())
        taskRec = context.tedDao.getTask(taskId);
        print(taskRec.toString() + " startTs=" + MiscUtils.toTimeString(taskRec.startTs));
        assertEquals("NEW", taskRec.status);

        // 2. after 1 day - must go to error
        print("Setting createTs to > 1 day ago");
        dao_setCreateTs(taskId, new Date(System.currentTimeMillis() - 24 * 60 * 61 * 1000));
        dao_setNextTs(taskId, new Date(System.currentTimeMillis() - 2 * 1000));

        processChannelTasks();
        Thread.sleep(20);

        taskRec = context.tedDao.getTask(taskId);
        print(taskRec.toString() + " startTs=" + MiscUtils.toTimeString(taskRec.startTs));
        assertEquals("ERROR", taskRec.status);

        dao_cleanupTasks(taskName);

    }

    // if task TEST03-02 working > 40 min (setup in config), then it should be set to ERROR
    @Test
    public void test02WorkTimeoutCancel() {
        String taskName = "TEST03-02";
        dao_cleanupAllTasks();

        // with timeout 40 minutes
        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));

        Long taskId;
        TaskRec taskRec;

        driver.createTask(taskName, null, null, null);

        // set status to work
        List<TaskRec> tasks = context.tedDao.reserveTaskPortion(new HashMap<String, Integer>(){{put("MAIN",1);}}, new Tick(1));
        taskId = tasks.get(0).taskId;

        taskRec = context.tedDao.getTask(taskId);
        //print(taskRec.toString() + " startTs=" + taskRec.startTs);
        assertEquals("WORK", taskRec.status);
        assertNull(taskRec.finishTs); // finish time is not set in beginning

        // change startTs to test work timeout - more than 40 minutes - should go to ERROR
        dao_setStartTs(taskId, new Date(System.currentTimeMillis() - 41 * 60 * 1000));
        taskRec = context.tedDao.getTask(taskId);
        print(taskRec.toString() + " startTs=" + MiscUtils.toTimeString(taskRec.startTs));
        context.maintenanceManager.processMaintenanceTasks();
        taskRec = context.tedDao.getTask(taskId);
        assertEquals("RETRY", taskRec.status);
        assertEquals("Too long in status [work](3)", taskRec.msg);
    }

    @Test
    public void test022WorkTimeout_stillWorking() {
        String taskName = "TEST03-02";
        dao_cleanupAllTasks();

        // with timeout 40 minutes
        driver.registerTaskConfig(taskName, forProcessor(new TestProcessorOkSleep(200)));

        Long taskId;
        TaskRec taskRec;

        taskId = driver.createTask(taskName, null, null, null);

        processChannelTasks();

        // set status to work
        taskRec = context.tedDao.getTask(taskId);
        assertEquals("WORK", taskRec.status);
        assertNull(taskRec.finishTs); // finish time is not set in beginning

        // hack - make task as "long-running" (make it old)
        ChannelThreadPoolExecutor pool = (ChannelThreadPoolExecutor)(context.registry.getChannel("MAIN").workers);
        Entry<Thread, MeasuredRunnable> runEntry = pool.threads.entrySet().iterator().next();
        runEntry.setValue(new MeasuredRunnable(runEntry.getValue().runnable, System.nanoTime() - 600_000_000_000L));

        // check task - should increase finishTs
        context.maintenanceManager.postponeLongRunningTaskTimeout();
        TaskRec taskRecV2 = context.tedDao.getTask(taskId);
        assertTrue("finishTs must be postponed, as task is still running", taskRecV2.finishTs.after(new Date()));

        // try again - should be postponed
        sleepMs(20);
        context.maintenanceManager.postponeLongRunningTaskTimeout();
        TaskRec taskRecV3 = context.tedDao.getTask(taskId);
        assertTrue("finishTs must be postponed again", taskRecV3.finishTs.after(taskRecV2.finishTs));

        // kill the task and try again - in db should not postpone
        runEntry.getKey().interrupt();
        sleepMs(20);
        context.maintenanceManager.postponeLongRunningTaskTimeout();
        TaskRec taskRecV4 = context.tedDao.getTask(taskId);
        assertEquals(taskRecV4.finishTs.getTime(), taskRecV3.finishTs.getTime());

        dao_cleanupAllTasks();
    }


    @Test
    public void test03DeleteOldTasks() {
        // oldTaskArchiveDays = 5

        String taskName = "TEST03-03";
        dao_cleanupAllTasks();

        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));

        Long taskId = driver.createTask(taskName, null, null, null);

        // check not so old tasks (should not be deleted)
        //
        dao_setDoneAndOld(taskId, 4);

        TaskRec taskRec = context.tedDao.getTask(taskId);
        //print(taskRec.toString() + " startTs=" + taskRec.startTs);
        assertEquals("DONE", taskRec.status);
        assertEquals(5, driver.getContext().config.oldTaskArchiveDays()); // from config
        context.tedDao.maintenanceDeleteTasks(driver.getContext().config.oldTaskArchiveDays(), null);

        taskRec = context.tedDao.getTask(taskId); // should still exists

        // check older tasks (should be deleted)
        //
        dao_setDoneAndOld(taskId, 6);
        context.tedDao.maintenanceDeleteTasks(driver.getContext().config.oldTaskArchiveDays(), null);

        taskRec = context.tedDao.getTask(taskId);
        assertNull("task should not exist", taskRec);

    }


    @Test
    public void test04ReindexQuickchk() {
        if (TestConfig.testDbType != TedDbType.POSTGRES) {
            logger.info("Skip test04ReindexQuickchk as it is for PostgreSQL only");
            return;
        }
        boolean success = context.tedDaoExt.maintenanceRebuildIndex();
        assertTrue("Failed to rebuild quickchk index", success);
    }

    @Ignore
    @Test
    public void test04MoveToArchive() throws InterruptedException {
        if (TestConfig.testDbType != TedDbType.POSTGRES) {
            logger.info("Skip test04MoveToArchive as it is for PostgreSQL only");
            return;
        }

        String taskName = "TEST03-03";

        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));

        Long taskId = driver.createTask(taskName, "test arch", null, null);
        dao_setDoneAndOld(taskId, 2);

        context.tedDaoExt.maintenanceMoveDoneTasks(archTable, 35);

        TaskRec res = dao_getArchTask(taskId);
        assertNotNull("task should exists in arch", res);

        res = context.tedDao.getTask(taskId);
        assertNull("task should not exist in tedtask", res);

        context.tedDao.maintenanceDeleteTasks(1, archTable);
        res = dao_getArchTask(taskId);
        assertNull("task should not exist in arch after delete", res);

    }

    @Ignore
    @Test
    public void test05DeleteFromArchive() {
        if (TestConfig.testDbType != TedDbType.POSTGRES) {
            logger.info("Skip test05DeleteFromArchive as it is for PostgreSQL only");
            return;
        }
        context.tedDao.maintenanceDeleteTasks(1, archTable);
    }

    @Ignore
    @Test
    public void test06DeleteFromTedTask() {
        String taskName = "TEST03-03";

        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));

        Long taskId = driver.createTask(taskName, "test task delete", null, null);
        dao_setDoneAndOld(taskId, 2);

        context.tedDao.maintenanceDeleteTasks(1, null);

        TaskRec res = context.tedDao.getTask(taskId);
        assertNull("task should not exist in tedtask", res);
    }

    void processChannelTasks() {
        context.taskManager.processChannelTasks(new Tick(1));
    }
}
