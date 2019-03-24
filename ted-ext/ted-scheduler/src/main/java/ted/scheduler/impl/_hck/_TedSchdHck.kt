package ted.driver

import ted.driver.sys.TedDriverImpl

object _TedSchdHck {
    fun getTedDriverImpl(tedDriver: TedDriver): TedDriverImpl {
        return tedDriver.tedDriverImpl
    }
}
