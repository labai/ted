package ted.driver.sys;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.sys.Registry.Channel;
import ted.driver.sys.TestTedProcessors.TestProcessorOk;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Augustus
 *         created on 2016.09.20
 */
//@Ignore
public class I02MultiTest extends TestBase {
    private final static Logger logger = LoggerFactory.getLogger(I02MultiTest.class);
    private TedDriverImpl driver1;
    private TedDriverImpl driver2;

    @Override
    protected TedDriverImpl getDriver() { return driver1; }

    private Channel channel1;
    private Channel channel2;

    @Before
    public void init() {
        // driver1
        driver1 = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID);
        channel1 = driver1.getContext().registry.getChannel("MAIN");
        ThreadPoolExecutor executor1 = channel1.workers;
        executor1.setThreadFactory(new ThreadFactory() {
            private int counter = 0;
            @Override
            public Thread newThread(Runnable runnable) {
                return new Thread(runnable, "Ted1-" + ++counter);
            }
        });
        executor1.setCorePoolSize(2);
        executor1.setMaximumPoolSize(2);
        // driver2
        driver2 = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID);
        channel2 = driver2.getContext().registry.getChannel("MAIN");
        ThreadPoolExecutor executor2 = channel2.workers;
        executor2.setThreadFactory(new ThreadFactory() {
            private int counter = 0;
            @Override
            public Thread newThread(Runnable runnable) {
                return new Thread(runnable, "Ted2-" + ++counter);
            }
        });
        executor2.setCorePoolSize(2);
        executor2.setMaximumPoolSize(2);
    }

    // test 2 instances
    // 1-st will take 20 tasks of 30 (2 workers * 10), 2-nd - remaining 10
    @Ignore
    @Test
    public void test01FullQueue() {
        String taskName = "TEST02-01";
        dao_cleanupAllTasks();

        driver1.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));
        driver2.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));

        for (int i = 0; i < 30; i++) {
            Long taskId = driver1.createTask(taskName, null, "num-" + i, null);
        }

        // will start parallel
        driver1.getContext().taskManager.processChannelTasks();
        TestUtils.print("Driver1 active="+ channel1.workers.getActiveCount() + " queue=" + channel1.workers.getQueue().size());
        driver2.getContext().taskManager.processChannelTasks();
        TestUtils.print("Driver2 active="+ channel2.workers.getActiveCount() + " queue=" + channel2.workers.getQueue().size());

        for (int i = 0; i < 10; i++) {
            TestUtils.sleepMs(600);
            driver2.getContext().taskManager.processChannelTasks();
        }
        TestUtils.print("Exit");
    }
}
