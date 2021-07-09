package ted.driver.sys;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedDbType;
import ted.driver.sys.PrimeInstance.CheckPrimeParams;
import ted.driver.sys.QuickCheck.CheckResult;
import ted.driver.sys.QuickCheck.Tick;
import ted.driver.sys.SqlUtils.DbType;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static ted.driver.sys.TestUtils.print;
import static ted.driver.sys.TestUtils.sleepMs;

public class I08PrimeInstanceTest extends TestBase {
    private final static Logger logger = LoggerFactory.getLogger(I08PrimeInstanceTest.class);

    private TedDriverImpl driver;
    private TedDao tedDao;
    private TedDaoExt tedDaoExt;

    private Gson gson = new GsonBuilder().setPrettyPrinting().create();

    @Override
    protected TedDriverImpl getDriver() { return driver; }

    @Before
    public void init() throws IOException {
        Assume.assumeTrue("For PostgreSQL only", TestConfig.testDbType == TedDbType.POSTGRES);

        Properties properties = TestUtils.readPropertiesFile("ted-I08.properties");
        this.driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID, properties);
        this.tedDao = driver.getContext().tedDao;
        this.tedDaoExt = driver.getContext().tedDaoExt;
        //this.context = driver.getContext();
    }


    @Test
    public void testPrimeInit() {
        // delete if exists
        dao_execSql("delete from tedtask where system = '" + TestConfig.SYSTEM_ID + "' and name = 'TED_PRIME'");

        Long primeTaskId;
        try {
            primeTaskId = tedDaoExt.findPrimeTaskId();
            if (tedDao.getDbType() != DbType.POSTGRES)
                fail("prime instance is not for Oracle");
        } catch (IllegalStateException e) {
            if (tedDao.getDbType() != DbType.POSTGRES)
                return;
            throw e;
        }
        print("created primeTaskId=" + primeTaskId + " inst=" + getContext().config.instanceId());
        assertNotNull(primeTaskId);
        //Assert.assertEquals(11L, (long)primeTaskId);

        Long primeTaskId2 = tedDaoExt.findPrimeTaskId();
        print("tried again primeTaskId=" + primeTaskId2);
        assertNotNull(primeTaskId2);
        Assert.assertEquals((long)primeTaskId, (long)primeTaskId);

        // insert next one
        String sql = "insert into tedtask(taskid, system, name, status, channel, startts, msg)"
            + " values (199, '" + TestConfig.SYSTEM_ID + "', 'TED_PRIME', 'SLEEP', 'TedNO', null, 'test 2')";
        dao_execSql(sql);

        try {
            primeTaskId2 = tedDaoExt.findPrimeTaskId();
            fail("should rise exception");
        } catch (IllegalStateException e) {
            // ok
            print("got exception as expected");
        }

        // cleanup
        dao_execSql("delete from tedtask where taskid = 199");
    }

    @Test
    public void testPrime() {
        Long tmpId;
        try {
            tmpId = tedDaoExt.findPrimeTaskId();
            if (tedDao.getDbType() == DbType.ORACLE)
                fail("prime instance is not for Oracle");
        } catch (IllegalStateException e) {
            if (tedDao.getDbType() == DbType.ORACLE)
                return;
            throw e;
        }
        final Long primeTaskId = tmpId;

        CheckPrimeParams checkPrimeParams = new CheckPrimeParams() {
            public boolean isPrime() { return true; }
            public String instanceId() { return "abra1"; }
            public long primeTaskId() { return primeTaskId; }
            public int postponeSec() { return 1; }

        };
        List<CheckResult> res = tedDao.quickCheck(checkPrimeParams, new Tick(1));
        print(gson.toJson(res));

        sleepMs(20);

        boolean isPrime = tedDaoExt.becomePrime(primeTaskId, "abra1", 5);
        print("isPrime=" + isPrime);
        assertTrue("take 1st", isPrime);
        sleepMs(20);
        boolean isPrime2 = tedDaoExt.becomePrime(primeTaskId, "abra2", 5);
        print("isPrime=" + isPrime2);
        assertFalse("cannot take 2nd", isPrime2);

    }
}
