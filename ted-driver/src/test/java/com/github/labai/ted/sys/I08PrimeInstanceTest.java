package com.github.labai.ted.sys;

import com.github.labai.ted.sys.JdbcSelectTed.SqlParam;
import com.github.labai.ted.sys.PrimeInstance.CheckPrimeParams;
import com.github.labai.ted.sys.QuickCheck.CheckResult;
import com.github.labai.ted.sys.TedDaoAbstract.DbType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static com.github.labai.ted.sys.TestConfig.SYSTEM_ID;
import static com.github.labai.ted.sys.TestUtils.print;
import static com.github.labai.ted.sys.TestUtils.sleepMs;
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
		Properties properties = new Properties();
		String propFileName = "ted-I08.properties";
		InputStream inputStream = TestBase.class.getClassLoader().getResourceAsStream(propFileName);
		if (inputStream == null)
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		properties.load(inputStream);

		this.driver = new TedDriverImpl(TestConfig.testDbType, TestConfig.getDataSource(), SYSTEM_ID, properties);
		this.tedDao = driver.getContext().tedDao;
		//this.context = driver.getContext();
	}

	private void dao_execSql (String sql) {
		((TedDaoAbstract)getContext().tedDao).execute("test", sql, Collections.<SqlParam>emptyList());
	}

	@Test
	public void testPrimeInit() throws Exception {
		// delete if exists
		dao_execSql("delete from tedtask where system = '" + SYSTEM_ID + "' and name = 'TED:PRIME'");

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
		print("created primeTaskId=" + primeTaskId + " inst=" + getContext().config.instanceId());
		Assert.assertNotNull(primeTaskId);
		//Assert.assertEquals(11L, (long)primeTaskId);

		Long primeTaskId2 = tedDao.findPrimeTaskId();
		print("tried again primeTaskId=" + primeTaskId2);
		Assert.assertNotNull(primeTaskId2);
		Assert.assertEquals((long)primeTaskId, (long)primeTaskId);

		// insert next one
		String sql = "insert into tedtask(taskid, system, name, status, channel, startts, msg)"
				+ " values (15, '" + SYSTEM_ID + "', 'TED:PRIME', 'SLEEP', 'TED', null, 'test 2')";
		dao_execSql(sql);

		try {
			primeTaskId2 = tedDao.findPrimeTaskId();
			fail("should rise exception");
		} catch (IllegalStateException e) {
			// ok
			print("got exception as expected");
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
		print(gson.toJson(res));

		sleepMs(20);

		boolean isPrime = tedDao.becomePrime(primeTaskId, "abra1");
		print("isPrime=" + isPrime);
		assertTrue("take 1st", isPrime);
		sleepMs(20);
		boolean isPrime2 = tedDao.becomePrime(primeTaskId, "abra2");
		print("isPrime=" + isPrime2);
		assertFalse("cannot take 2nd", isPrime2);

	}
}
