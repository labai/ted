package labai.ted.sys;

import labai.ted.sys.RetryConfig.PeriodPatternConfig;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * @author Augustus
 *         created on 2016.09.21
 */
public class T04FunctionsTest {
	private final static Logger logger = LoggerFactory.getLogger(T04FunctionsTest.class);

	private boolean hasNonAscii(String str) {
		if (str == null)
			return false;
		return !str.matches("\\A\\p{ASCII}*\\z");
	}

	@Test
	public void testCallOracle() throws Exception {
		//TaskRec taskRec = driver.getContext().tedDao.getTask(1508);
		//print(taskRec.toString());
		Pattern p = Pattern.compile("[^a-z0-9 \\_\\-\\.]", Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher("abra -._kadabra");
		boolean b = m.find();
		TestUtils.print("found:" + b);

		TestUtils.print("has nonascii:" + hasNonAscii("abra-\nkadabra ę"));


		char nonAscii = 0x00FF;
		String asciiText = "Hello";
		String nonAsciiText = "Buy: " + nonAscii;
		System.out.println(asciiText.matches("\\A\\p{ASCII}*\\z"));
		System.out.println(nonAsciiText.matches("\\A\\p{ASCII}*\\z"));
	}

	@Test
	public void testRetryPolicy() throws Exception {
		// test getNextRetryPauseSec
		PeriodPatternConfig policy = new PeriodPatternConfig("2s,10s*2,30s*5,10m*3,1h*5");
		assertEquals(2,  (long)policy.getNextRetryPauseSec(1));
		assertEquals(10, (long)policy.getNextRetryPauseSec(2));
		assertEquals(10, (long)policy.getNextRetryPauseSec(3));
		assertEquals(30, (long)policy.getNextRetryPauseSec(4));
		assertEquals(30, (long)policy.getNextRetryPauseSec(8));
		assertEquals(600, (long)policy.getNextRetryPauseSec(9));
		assertEquals(3600, (long)policy.getNextRetryPauseSec(12));
		assertEquals(3600, (long)policy.getNextRetryPauseSec(16));
		assertNull(policy.getNextRetryPauseSec(17));

		try {
			policy.getNextRetryPauseSec(0);
			fail("expecting exception");
		} catch (Exception e) {
		}

		// test pattern parser

		// spaces - ok
		policy = new PeriodPatternConfig(" 2s, 10s * 2, 30s*5,10m*3,1h*5");
		assertEquals(3600, (long)policy.getNextRetryPauseSec(16));

		try {
			new PeriodPatternConfig("2,10s*2,30s*5,10m*3,1h*5");
			fail("no suffix (s, m, h) - must fail");
		} catch (Exception e) {
			//e.printStackTrace();
		}

		try {
			new PeriodPatternConfig(",10s*2,30s*5,10m*3,1h*5");
			fail("empty entry - must fail");
		} catch (Exception e) {
			//e.printStackTrace();
		}

		try {
			new PeriodPatternConfig(" ");
			fail("empty - must fail");
		} catch (Exception e) {
			//e.printStackTrace();
		}

		try {
			new PeriodPatternConfig("2k,10s*2,30s*5,10m*3,1h*5");
			fail("invalid suffix - must fail");
		} catch (Exception e) {
			//e.printStackTrace();
		}

		// dispersion
		//
		assertEquals(5, PeriodPatternConfig.parsePeriodDispersion("2s,10s*2,30s*5,10m*3,1h*5;dispersion=5"));
		assertEquals(5, PeriodPatternConfig.parsePeriodDispersion("2s,10s*2,30s*5,10m*3,1h*5;dispersion = 5"));
		assertEquals(0, PeriodPatternConfig.parsePeriodDispersion("2s,10s*2,30s*5,10m*3,1h*5"));
	}


	@Test
	public void testParseChannelProperties() throws Exception {
		Properties properties = new Properties();
		properties.put("abra.kadabra", "abra.kadabra");
		properties.put("abra.channel.MAIN", "abra.channel.MAIN");
		properties.put("abra.channel.MAIN.workerCount", "10");
		properties.put("abra.channel.MAIN.workerMax", "15");
		properties.put("abra.channel.CHAN1", "");
		properties.put("abra.channel.CHAN2.workerMax.bla", "5");

		Map<String, Properties> result = ConfigUtils.getShortPropertiesByPrefix(properties, "abra.channel.");
		assertEquals(3, result.size());
		Assert.assertTrue(result.containsKey("MAIN"));
		Assert.assertTrue(result.containsKey("CHAN1"));
		assertEquals(2, result.get("MAIN").size());
		Assert.assertTrue(result.get("MAIN").containsKey("workerMax"));
		TestUtils.print(result.toString());

	}


	@Test
	public void testMakeShortName() throws Exception {
		TestUtils.print(Registry.makeShortName(""));
		TestUtils.print(Registry.makeShortName("A"));
		TestUtils.print(Registry.makeShortName("a"));
		assertEquals("ASJB0", Registry.makeShortName("_ASA;pa ssdp-a"));

	}
}
