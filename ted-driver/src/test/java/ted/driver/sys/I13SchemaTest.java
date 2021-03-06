package ted.driver.sys;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.TedDriver;
import ted.driver.TedTaskManager;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.TedDriverImpl.TedContext;
import ted.driver.sys.TestTedProcessors.TestProcessorOk;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author Augustus
 *         created on 2019.11.26
 *
 * require created zz_tedtask table
 */
@Ignore
public class I13SchemaTest extends TestBase {
    private final static Logger logger = LoggerFactory.getLogger(I13SchemaTest.class);

    private TedDriverImpl driver1impl;
    private TedDriver driver2;
    private TedTaskManager taskHelper2;
    private TedContext context;

    @Override
    protected TedDriverImpl getDriver() { return driver1impl; }

    @Before
    public void init() throws IOException {
        Properties properties = TestUtils.readPropertiesFile("ted-I13.properties");
        properties.setProperty("ted.systemId", TestConfig.SYSTEM_ID);
        driver1impl = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID, properties);
        driver2 = new TedDriver(TestConfig.testDbType, TestConfig.getDataSource(), properties);
        taskHelper2 = new TedTaskManager(driver2);

        this.context = driver1impl.getContext();
        dao_cleanupAllTasks();
    }

    @Test
    public void test01CreateTask() {
        String taskName = "TEST01-01";
        driver2.registerTaskConfig(taskName, TestTedProcessors.forClass(TestProcessorOk.class));

        Long taskId = taskHelper2.getTaskFactory().taskBuilder(taskName)
            .data("test-data")
            .key1("test-key1")
            .key2("test-key2")
            .create();

        TaskRec taskRec = driver1impl.getContext().tedDao.getTask(taskId);
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
        assertNotNull(taskRec.nextTs);
        assertEquals(0, (int)taskRec.retries);
        assertNull(taskRec.startTs);
        assertNull(taskRec.msg);
        assertNull(taskRec.finishTs);

    }

}
