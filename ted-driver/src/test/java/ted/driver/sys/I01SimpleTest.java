package ted.driver.sys;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedProcessor;
import ted.driver.Ted.TedStatus;
import ted.driver.TedResult;
import ted.driver.TedTask;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.QuickCheck.Tick;
import ted.driver.sys.SqlUtils.DbType;
import ted.driver.sys.TedDriverImpl.TedContext;
import ted.driver.sys.TestTedProcessors.OnTaskFinishListener;
import ted.driver.sys.TestTedProcessors.TestProcessorException;
import ted.driver.sys.TestTedProcessors.TestProcessorOk;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static ted.driver.sys.TestTedProcessors.forClass;
import static ted.driver.sys.TestUtils.awaitUntilTaskFinish;
import static ted.driver.sys.TestUtils.print;
import static ted.driver.sys.TestUtils.sleepMs;

/**
 * @author Augustus
 *         created on 2016.09.19
 */
public class I01SimpleTest extends TestBase {
    private final static Logger logger = LoggerFactory.getLogger(I01SimpleTest.class);

    private TedDriverImpl driver;
    private TedContext context;

    @Override
    protected TedDriverImpl getDriver() { return driver; }

    @Before
    public void init() throws IOException {
        Properties properties = TestUtils.readPropertiesFile("ted-I01.properties");
        driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID, properties);
        this.context = driver.getContext();
        dao_cleanupAllTasks();
    }

    private static class DateVal {
        Date dateVal;
    }

    @Test
    public void testCompareNows() {
        String sql = "select $now as dateVal";
        sql = sql.replace("$now", context.tedDao.getDbType().sql().now());
        if (context.tedDao.getDbType() == DbType.ORACLE)
            sql += " from dual";
        Date before = new Date();
        List<DateVal> res = ((TedDaoAbstract)context.tedDao).selectData("getnow", sql, DateVal.class, Collections.emptyList());
        Date sqlNow = res.get(0).dateVal;
        //Date sqlNow =  new Date(res.get(0).dateVal.getTime());
        Date after = new Date();
        print(TestUtils.shortTime(before) + " - " + TestUtils.shortTime(sqlNow) + " - " + TestUtils.shortTime(after));
        print(before.getTime() + " - " + sqlNow.getTime() + " - " + after.getTime());

        try {
            assertTrue("clocks differs between db server and this machine(1)", before.compareTo(sqlNow) <= 0);
            assertTrue("clocks differs between db server and this machine(2)", sqlNow.compareTo(after) <= 0);
        } catch (AssertionError e) {
            logger.warn("clocks differs between db server and this machine");
        }
    }

    @Ignore // data not limited
    @Test
    public void test01ClobVarchar() {
        String taskName = "TEST01-01";
        driver.registerTaskConfig(taskName, forClass(TestProcessorOk.class));

        // long string
        String param = "";
        for (int i = 0; i < 1000; i++) {
            param += i + " ---------------------------------------------------------------------------------------------------- " + i + "\n";
        }
        Long taskId = null;
        try {
            taskId = driver.createTask(taskName, ("test long param\n" + param), null, null);
            fail("Expected string too long exception");
        } catch (Exception e) {
        }

//		TaskRec taskRec = driver.getContext().tedDao.getTask(taskId);
//		assertEquals("NEW", taskRec.status);
//		assertEquals("test long param\n" + param, new String(taskRec.data));
//		driver.getContext().tedDao.setStatus(taskId, TedStatus.DONE, "test ok");
//		taskRec = driver.getContext().tedDao.getTask(taskId);
//		assertEquals("DONE", taskRec.status);

    }

    @Test
    public void test01CreateTask() {
        String taskName = "TEST01-01";
        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));

        Long taskId = driver.createTask(taskName, "test-data", "test-key1", "test-key2");

        TaskRec taskRec = context.tedDao.getTask(taskId);
        assertEquals("NEW", taskRec.status);
        assertEquals(TestConfig.SYSTEM_ID, taskRec.system);
        assertEquals("TEST01-01", taskRec.name);
        //assertEquals("test-version", taskRec.version);
        assertEquals("test-data", new String(taskRec.data));

        assertEquals("test-key1", taskRec.key1);
        assertEquals("test-key2", taskRec.key2);
        assertEquals("MAIN", taskRec.channel);
        assertNotNull(taskRec.taskId);
        assertNotNull(taskRec.createTs);
        //assertNotNull(taskRec.startTs);
        assertNotNull(taskRec.nextTs);
        assertEquals(0, (int)taskRec.retries);
        assertNull(taskRec.startTs);
        assertNull(taskRec.msg);
        assertNull(taskRec.finishTs);
        //assertNull(taskRec.result);

        //context.tedDao.setStatus(taskId, TedStatus.DONE, "test-msg", "test-result".getBytes());
        context.tedDao.setStatus(taskId, TedStatus.DONE, "test-msg");
        taskRec = context.tedDao.getTask(taskId);
        assertEquals("DONE", taskRec.status);
        assertEquals("test-msg", taskRec.msg);
        //assertEquals("test-result", new String(taskRec.result));

    }

    @Test
    public void test02CreateAndDone() {
        String taskName = "TEST01-02";

        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));

        final Long taskId = driver.createTask(taskName, null, null, null);

        TaskRec taskRec = context.tedDao.getTask(taskId);
        print(taskRec.toString());
        assertEquals("NEW", taskRec.status);

        // will start parallel
        processChannelTasks();

        taskRec = context.tedDao.getTask(taskId);
        print(taskRec.toString());
        assertEquals("WORK", taskRec.status);
        assertNull(taskRec.finishTs);

        awaitUntilTaskFinish(driver, taskId, 500);

        taskRec = context.tedDao.getTask(taskId);
        assertEquals("DONE", taskRec.status);
        assertNotNull(taskRec.finishTs);

    }

    @Test
    public void test03CreateAndException() {
        String taskName = "TEST01-03";

        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorException.class));

        final Long taskId = driver.createTask(taskName, null, null, null);

        TaskRec taskRec = context.tedDao.getTask(taskId);
        print(taskRec.toString());
        assertEquals("NEW", taskRec.status);

        // will start parallel
        processChannelTasks();

        taskRec = context.tedDao.getTask(taskId);
        print(taskRec.toString());
        assertEquals("WORK", taskRec.status);

        awaitUntilTaskFinish(driver, taskId, 300);

        taskRec = context.tedDao.getTask(taskId);
        assertEquals("ERROR", taskRec.status);
        assertEquals("Catch: Test runtime exception", taskRec.msg);

    }

    public static class Test01ProcessorRetry implements TedProcessor {
        @Override
        public TedResult process(TedTask task)  {
            logger.info(this.getClass().getSimpleName() + " process");
            sleepMs(20);
            if (task.getRetries() >= 2)
                return TedResult.retry("done");
            return TedResult.retry("temporary problems");
        }
    }


    @Test
    public void test04CreateAndRetry() throws Exception {
        String taskName = "TEST01-04";
        final CountDownLatch latch = new CountDownLatch(1);
        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(Test01ProcessorRetry.class));

        final Long taskId = driver.createTask(taskName, null, null, null);

        driver.setMetricsRegistry(new OnTaskFinishListener((ataskId, status) -> {
            if (ataskId != taskId) return;
            latch.countDown();
        }));

        TaskRec taskRec = context.tedDao.getTask(taskId);
        //print(taskRec.toString());
        assertEquals("NEW", taskRec.status);

        // will start parallel
        processChannelTasks();

        taskRec = context.tedDao.getTask(taskId);
        //print(taskRec.toString());
        assertEquals("WORK", taskRec.status);

        latch.await(200, TimeUnit.MILLISECONDS);
        context.taskManager.flushStatuses();

        taskRec = context.tedDao.getTask(taskId);
        long deltaMs = taskRec.nextTs.getTime() - System.currentTimeMillis();
        print(taskRec.toString() + " deltaMs:" + deltaMs);
        assertEquals("RETRY", taskRec.status);
        assertTrue("next ts in 12 +/- 15% sec", (deltaMs > 9000 && deltaMs < 14000));
        assertEquals(1, (long)taskRec.retries);

    }


    @Test
    public void test05GetPortion() {
        String taskName = "TEST01-05";
        String taskName2 = "TEST01-05-2";
        context.registry.registerChannel("TEST1", 5, 100);
        context.registry.registerChannel("TEST2", 5, 100);
        //driver.registerTaskConfig(taskName, forClass(Test01ProcessorRetry.class), 1, null, "TEST1");

        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(Test01ProcessorRetry.class));
        driver.registerTaskConfig(taskName2, TestTedProcessors.forClass(Test01ProcessorRetry.class));

        Long taskId = driver.createTask(taskName, null, null, null);
        Long taskId2 = driver.createTask(taskName2, null, null, null);

        TaskRec taskRec = context.tedDao.getTask(taskId);
        //print(taskRec.toString());
        assertEquals("NEW", taskRec.status);


        Map<String, Integer> channelSizes = new HashMap<>();
        channelSizes.put("TEST1", 3);
        channelSizes.put("TEST2", 4);
        // add few more to check subselect chunks..
        channelSizes.put("TEST3", 1);
        channelSizes.put("TEST4", 1);
        channelSizes.put("TEST5", 1);
        channelSizes.put("TEST6", 1);
        channelSizes.put("TEST7", 1);
        channelSizes.put("TEST8", 1);
        channelSizes.put("TEST9", 1);
        channelSizes.put("TST10", 1);
        channelSizes.put("TST11", 1);
        List<TaskRec> list = context.tedDao.reserveTaskPortion(channelSizes, new Tick(1));
        assertEquals(2, list.size());
        Map<String, List<TaskRec>> map = list.stream().collect(Collectors.groupingBy(it -> it.name));
        assertEquals(1, map.get(taskName).size());
        assertEquals(1, map.get(taskName2).size());
    }


    @Test
    public void test06GetPortionLocked() throws Exception {
        if (context.tedDao.getDbType() == DbType.HSQLDB) {
            logger.warn("Skipped, as HSQLDB do not support locking");
            return;
        }

        String taskName = "TEST01-05";
        dao_cleanupAllTasks();

        context.registry.registerChannel("TEST1", 5, 100);
        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(Test01ProcessorRetry.class));

        // create 2 tasks, then lock 1 of them
        driver.createTask(taskName, null, null, null);
        final Long lockTaskId = driver.createTask(taskName, null, null, null);
        TaskRec taskRec = context.tedDao.getTask(lockTaskId);
        assertEquals("NEW", taskRec.status);

        logger.info("Before lock");
        new Thread(() -> {
            dao_lockAndSleep(lockTaskId, "0.2");
            logger.info("After lock");
        }).start();

        Thread.sleep(20);
        logger.info("Before next");

        Map<String, Integer> channelSizes = new HashMap<>();
        channelSizes.put("TEST1", 3);
        List<TaskRec> list = context.tedDao.reserveTaskPortion(channelSizes, new Tick(1));
        assertEquals(1, list.size());
        assertEquals(taskName, list.get(0).name);
        logger.info("Done");
    }


    @Test
    public void test07CheckNextTsLimit() {
        if (context.tedDao.getDbType() != DbType.POSTGRES) {
            logger.warn("Skipped for non PostgreSql");
            return;
        }
        String taskName = "TEST01-05";
        context.registry.registerChannel("TEST1", 5, 100);

        driver.registerTaskConfig(taskName, TestTedProcessors.forClass(Test01ProcessorRetry.class));

        Long taskId = driver.createTask(taskName, null, null, null);
        Long taskId2 = driver.createTask(taskName, null, null, null);

        // make taskId2 old
        String sql = "update tedtask set nextts = $now - $interval where taskid = " + taskId2;
        sql = sql.replace("$now", getDbType().sql().now());
        sql = sql.replace("$interval", getDbType().sql().intervalSeconds(9999));
        dao_execSql(sql);

        TaskRec taskRec = context.tedDao.getTask(taskId);
        assertEquals("NEW", taskRec.status);

        Map<String, Integer> channelSizes = new HashMap<>();
        channelSizes.put("TEST1", 3);
        List<TaskRec> list = context.tedDao.reserveTaskPortion(channelSizes, tickWithLimited(true));
        assertEquals(1, list.size());

        // no more exists in last 10 minutes
        list = context.tedDao.reserveTaskPortion(channelSizes, tickWithLimited(true));
        assertEquals(0, list.size());

        // with checkLimited = false should find old
        list = context.tedDao.reserveTaskPortion(channelSizes, tickWithLimited(false));
        assertEquals(1, list.size());

    }

    private Tick tickWithLimited(boolean limitPeriod) {
        return new Tick(1) {{
            this.limitNextTs = limitPeriod;
        }};
    }

    void processChannelTasks() {
        context.taskManager.processChannelTasks(new Tick(1));
    }

/*	@Test
	public void test07CreateUniqueKey1() throws Exception {
		String taskName = "TEST01-07";

		driver.registerTaskConfig(taskName, TestUtils.forClass(Test01ProcessorRetry.class));

		Long taskId = driver.createTaskUniqueKey1(taskName, "row1", "key1", null);
		assertNotNull("should not created", taskId);
		taskId = driver.createTaskUniqueKey1(taskName, "row2", "key1", null);
		assertNull("should not be created due to key1 duplicate", taskId);
	}*/

    private void dao_lockAndSleep(long taskId, String sec) {
        if (context.tedDao instanceof TedDaoOracle) {
            ((TedDaoOracle) context.tedDao).selectFromBlock("dao_lockAndSleep",
                "declare " +
                    " v_till timestamp;" +
                    " begin" +
                    " update tedtask set status = status where taskid = " + taskId + ";" +
                    " select systimestamp + interval '" + sec + "' second into v_till from dual;" +
                    " loop " +
                    "   exit when systimestamp >= v_till;" + // sleep uses cpu..
                    " end loop;" +
                    " end;",
                Void.class, Collections.emptyList());
        } else if (context.tedDao instanceof TedDaoPostgres){
            ((TedDaoAbstract) context.tedDao).execute("dao_lockAndSleep",
                " update tedtask set status = status where taskid = " + taskId + "; SELECT pg_sleep(" + sec + ");", Collections.emptyList());
        } else if (context.tedDao instanceof TedDaoMysql){
            ((TedDaoAbstract) context.tedDao).execute("dao_lockAndSleep",
                " update tedtask set status = status where taskid = " + taskId + " and sleep(" + sec + ") = 0;", Collections.emptyList());
        } else {
            throw new IllegalStateException("TODO (something new?)");
        }

    }

}
