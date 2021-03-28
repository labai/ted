package ted.driver.sys;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.sys.Model.FieldValidator;
import ted.driver.sys.RetryConfig.PeriodPatternConfig;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static ted.driver.sys.TestUtils.print;

/**
 * @author Augustus
 *         created on 2016.09.21
 */
public class T04FunctionsTest {
    private final static Logger logger = LoggerFactory.getLogger(T04FunctionsTest.class);

    @Test
    public void testFieldValidatorFunctions() {
        assertFalse(FieldValidator.hasNonLetters(""));
        assertFalse(FieldValidator.hasNonLetters("abCD12"));
        assertTrue(FieldValidator.hasNonLetters("ą"));
        assertTrue(FieldValidator.hasNonLetters("a_B"));
        assertTrue(FieldValidator.hasNonLetters("a\nb"));
        assertTrue(FieldValidator.hasNonLetters("a b"));

        assertTrue(FieldValidator.hasInvalidChars("a b"));
        assertTrue(FieldValidator.hasInvalidChars("a\nb"));
        assertTrue(FieldValidator.hasInvalidChars("a*b"));
        assertTrue(FieldValidator.hasInvalidChars(" ab"));
        assertTrue(FieldValidator.hasInvalidChars("ab "));
        assertFalse(FieldValidator.hasInvalidChars("abra-._kadabra"));

        assertFalse(FieldValidator.hasNonAscii("abra-kadabra"));
        assertTrue(FieldValidator.hasNonAscii("abra-\nkadabra ę"));
    }

    @Test
    public void testRetryPolicy() {
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
    public void testParseChannelProperties() {
        Properties properties = new Properties();
        properties.put("abra.kadabra", "abra.kadabra");
        properties.put("abra.channel.MAIN", "abra.channel.MAIN");
        properties.put("abra.channel.MAIN.workerCount", "10");
        properties.put("abra.channel.MAIN.workerMax", "15");
        properties.put("abra.channel.CHAN1", "");
        properties.put("abra.channel.CHAN2.workerMax.bla", "5");

        Map<String, Properties> result = ConfigUtils.getShortPropertiesByPrefix(properties, "abra.channel.");
        assertEquals(3, result.size());
        assertTrue(result.containsKey("MAIN"));
        assertTrue(result.containsKey("CHAN1"));
        assertEquals(2, result.get("MAIN").size());
        assertTrue(result.get("MAIN").containsKey("workerMax"));
        print(result.toString());

    }


    @Test
    public void testMakeShortName() {
        print(Registry.makeShortName(""));
        print(Registry.makeShortName("A"));
        print(Registry.makeShortName("a"));
        assertEquals("ASJB0", Registry.makeShortName("_ASA;pa ssdp-a"));

    }

    @Test
    public void testSplitList() {
        List<String> list = MiscUtils.asList("1", "2", "3", "4", "5");
        assertEquals("[[1, 2], [3, 4], [5]]", TedDaoPostgres.splitList(list, 2).toString());
        assertEquals("[[1, 2, 3, 4, 5]]", TedDaoPostgres.splitList(list, 5).toString());

    }

}
