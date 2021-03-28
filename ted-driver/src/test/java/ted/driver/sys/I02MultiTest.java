package ted.driver.sys;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.TedResult;
import ted.driver.sys.Registry.Channel;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static ted.driver.sys.TestUtils.awaitTask;
import static ted.driver.sys.TestUtils.print;

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
    @Test
    public void testFullQueue() {
        String taskName = "TEST02-01";
        dao_cleanupAllTasks();

        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();

        driver1.registerTaskConfig(taskName, proc -> task -> {
            count1.getAndIncrement();
            logger.info("P1 process");
            return TedResult.done();
        });
        driver2.registerTaskConfig(taskName, proc -> task -> {
            count2.getAndIncrement();
            logger.info("P2 process");
            return TedResult.done();
        });

        for (int i = 0; i < 10; i++) {
            driver1.createTask(taskName, null, "num-" + i, null);
        }

        // will start parallel
        // at first will take 3 each of processors, next will take 6
        driver1.getContext().taskManager.processChannelTasks();
        print("Driver1 active="+ channel1.workers.getActiveCount() + " queue=" + channel1.workers.getQueue().size());
        driver2.getContext().taskManager.processChannelTasks();
        print("Driver2 active="+ channel2.workers.getActiveCount() + " queue=" + channel2.workers.getQueue().size());
        driver1.getContext().taskManager.processChannelTasks();
        print("Driver1 active="+ channel1.workers.getActiveCount() + " queue=" + channel1.workers.getQueue().size());

        awaitTask(200, () ->
            count1.get() + count2.get() >= 10
        );

        assertEquals("First should take 7 tasks", 7, count1.get());
        assertEquals("Second should take 3 tasks", 3, count2.get());

    }
}
