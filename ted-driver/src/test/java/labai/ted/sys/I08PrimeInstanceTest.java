package labai.ted.sys;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import labai.ted.Ted.TedDbType;
import labai.ted.sys.JdbcSelectTed.SqlParam;
import labai.ted.sys.PrimeInstance.CheckPrimeParams;
import labai.ted.sys.QuickCheck.CheckResult;
import labai.ted.sys.TedDaoAbstract.DbType;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class I08PrimeInstanceTest extends TestBase {
	private final static Logger logger = LoggerFactory.getLogger(I08PrimeInstanceTest.class);

	private TedDriverImpl driver;
	private TedDao tedDao;

	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

	@Override
	protected TedDriverImpl getDriver() { return driver; }

	@Before
	public void init() throws IOException {
		Assume.assumeTrue("Not for Oracle", TestConfig.testDbType == TedDbType.POSTGRES);

		Properties properties = TestUtils.readPropertiesFile("ted-I08.properties");
		this.driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), TestConfig.SYSTEM_ID, properties);
		this.tedDao = driver.getContext().tedDao;
		//this.context = driver.getContext();
	}

	private void dao_execSql (String sql) {
		((TedDaoAbstract)getContext().tedDao).execute("test", sql, Collections.<SqlParam>emptyList());
	}

	@Test
	public void testPrimeInit() throws Exception {
		// delete if exists
		dao_execSql("delete from tedtask where system = '" + TestConfig.SYSTEM_ID + "' and name = 'TED:PRIME'");

		Long primeTaskId = null;
		try {
			primeTaskId = tedDao.findPrimeTaskId();
			if (tedDao.getDbType() == DbType.ORACLE)
				fail("prime instance is not for Oracle");
		} catch (IllegalStateException e) {
			if (tedDao.getDbType() == DbType.ORACLE)
				return;
			throw e;
		}
		TestUtils.print("created primeTaskId=" + primeTaskId + " inst=" + getContext().config.instanceId());
		Assert.assertNotNull(primeTaskId);
		//Assert.assertEquals(11L, (long)primeTaskId);

		Long primeTaskId2 = tedDao.findPrimeTaskId();
		TestUtils.print("tried again primeTaskId=" + primeTaskId2);
		Assert.assertNotNull(primeTaskId2);
		Assert.assertEquals((long)primeTaskId, (long)primeTaskId);

		// insert next one
		String sql = "insert into tedtask(taskid, system, name, status, channel, startts, msg)"
				+ " values (15, '" + TestConfig.SYSTEM_ID + "', 'TED:PRIME', 'SLEEP', 'TedNO', null, 'test 2')";
		dao_execSql(sql);

		try {
			primeTaskId2 = tedDao.findPrimeTaskId();
			fail("should rise exception");
		} catch (IllegalStateException e) {
			// ok
			TestUtils.print("got exception as expected");
		}

		// cleanup
		dao_execSql("delete from tedtask where taskid = 15");
	}

	@Test
	public void testPrime() {
		Long tmpId;
		try {
			tmpId = tedDao.findPrimeTaskId();
			if (tedDao.getDbType() == DbType.ORACLE)
				fail("prime instance is not for Oracle");
		} catch (IllegalStateException e) {
			if (tedDao.getDbType() == DbType.ORACLE)
				return;
			throw e;
		}
		final Long primeTaskId = tmpId;

		CheckPrimeParams checkPrimeParams = new CheckPrimeParams() {
			public boolean isPrime() { return true; };
			public String instanceId() { return "abra1"; };
			public long primeTaskId() { return primeTaskId; }
			public int postponeSec() { return 1; };

		};
		List<CheckResult> res = tedDao.quickCheck(checkPrimeParams);
		TestUtils.print(gson.toJson(res));

		TestUtils.sleepMs(20);

		boolean isPrime = tedDao.becomePrime(primeTaskId, "abra1");
		TestUtils.print("isPrime=" + isPrime);
		assertTrue("take 1st", isPrime);
		TestUtils.sleepMs(20);
		boolean isPrime2 = tedDao.becomePrime(primeTaskId, "abra2");
		TestUtils.print("isPrime=" + isPrime2);
		assertFalse("cannot take 2nd", isPrime2);

	}
}
