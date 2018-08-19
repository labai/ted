package labai.ted.scheduler;

import labai.ted.sys.TedDriverImpl;
import org.junit.Assume;
import org.junit.Before;

/**
 * @author Augustus
 *         created on 2016.09.19
 */
public abstract class TestBase {

	protected abstract TedDriverImpl getDriver();

	@Before
	public void initCheck() {
		Assume.assumeTrue("Are tests enabled?", TestConfig.INT_TESTS_ENABLED);
	}

//	protected TedContext getContext() {
//		return getDriver().getContext();
//	}

}
