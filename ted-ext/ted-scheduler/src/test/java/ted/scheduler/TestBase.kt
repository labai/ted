package ted.scheduler

import ted.driver.sys.TedDriverImpl
import org.junit.Assume
import org.junit.Before

/**
 * @author Augustus
 * created on 2016.09.19
 */
abstract class TestBase {

    protected abstract val driver: TedDriverImpl

    @Before
    fun initCheck() {
        Assume.assumeTrue("Are tests enabled?", TestConfig.INT_TESTS_ENABLED)
    }

}
