package ted.driver.sys;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedDbType;
import ted.driver.TedResult;
import ted.driver.TedTask;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.PrimeInstance.CheckPrimeParams;
import ted.driver.sys.QuickCheck.CheckResult;
import ted.driver.sys.TedDriverImpl.TedContext;
import ted.driver.sys.TestTedProcessors.SingeInstanceFactory;
import ted.driver.sys.TestTedProcessors.TestProcessorOk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static ted.driver.sys.TestUtils.print;
import static ted.driver.sys.TestUtils.sleepMs;

public class I10NotificationTest extends TestBase {
    private final static Logger logger = LoggerFactory.getLogger(I10NotificationTest.class);

    private TedDriverImpl driver1;
    private TedDriverImpl driver2;

    private TedDao tedDao;

    @Override
    protected TedDriverImpl getDriver() { return driver1; }

    @Before
    public void init() {
        Assume.assumeTrue("Not for Oracle", TestConfig.testDbType == TedDbType.POSTGRES);

        driver1 = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID);
        driver2 = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID);

        tedDao = driver1.getContext().tedDao;

        dao_cleanupAllTasks();
        dao_cleanupPrime();
        dao_execSql("update tedtask set status = 'DONE', nextts = null where system = '" + TestConfig.SYSTEM_ID + "' and channel = 'TedIN' " +
            " and status <> 'DONE'");
    }

    private void dao_execSql (String sql) {
        ((TedDaoAbstract)getContext().tedDao).execute("test", sql, Collections.emptyList());
    }

    @Test
    public void test01SendNotification() {
        String taskName = "TEST10-1";

        final int[] calls = {0, 0};
        driver1.registerTaskConfig(taskName, new SingeInstanceFactory(new TestProcessorOk(){
            @Override
            public TedResult process(TedTask task) {
                calls[0]++;
                return super.process(task);
            }
        }));
        driver2.registerTaskConfig(taskName, new SingeInstanceFactory(new TestProcessorOk(){
            @Override
            public TedResult process(TedTask task) {
                calls[1]++;
                return super.process(task);
            }
        }));
        driver1.prime().enable();
        driver1.prime().init();
        driver2.prime().enable();
        driver2.prime().init();

        Long taskId = driver1.sendNotification(taskName, "test10");
        driver1.getContext().notificationManager.processNotifications();
        sleepMs(10);
        driver2.getContext().notificationManager.processNotifications();

        TaskRec taskRec = tedDao.getTask(taskId);
        print(taskRec.toString());
        assertEquals("NEW", taskRec.status);

        // set to expired
        dao_execSql("update tedtask set nextts = nextts - interval '10 seconds' where taskid = " + taskId);
        sleepMs(50);
        driver1.getContext().notificationManager.processNotifications();
        sleepMs(10);
        driver2.getContext().notificationManager.processNotifications();
        sleepMs(50);
        taskRec = tedDao.getTask(taskId);
        print(taskRec.toString());

        assertEquals("DONE", taskRec.status);
        assertEquals("1st instance called", 1, calls[0]);
        assertEquals("2st instance called", 1, calls[1]);
    }

    @Test
    public void test02SendNotification() {
        final TedDriverImpl locDriver1 = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID);
        final TedDriverImpl locDriver2 = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID);

        locDriver1.prime().enable();
        locDriver1.prime().init();
        locDriver2.prime().enable();
        locDriver2.prime().init();

        TedContext context1 = locDriver1.getContext();
        TedContext context2 = locDriver2.getContext();

        context1.tedDao = Mockito.mock(TedDaoPostgres.class);
        context1.notificationManager = Mockito.mock(NotificationManager.class);
        context2.tedDao = Mockito.mock(TedDaoPostgres.class);
        context2.notificationManager = Mockito.mock(NotificationManager.class);

        List<CheckResult> chkres = new ArrayList<>();
        chkres.add(new CheckResult("CHAN", Model.CHANNEL_NOTIFY, null));
        doReturn(chkres).when(context1.tedDao).quickCheck(isA(CheckPrimeParams.class), anyBoolean());
        doReturn(chkres).when(context2.tedDao).quickCheck(isA(CheckPrimeParams.class), anyBoolean());

        context1.quickCheck.quickCheck();
        context2.quickCheck.quickCheck();

        verify(context1.tedDao, times(1)).quickCheck(isA(CheckPrimeParams.class), anyBoolean());
        verify(context1.notificationManager, times(1)).processNotifications();
        verify(context2.tedDao, times(1)).quickCheck(isA(CheckPrimeParams.class), anyBoolean());
        verify(context2.notificationManager, times(1)).processNotifications();


    }

}
