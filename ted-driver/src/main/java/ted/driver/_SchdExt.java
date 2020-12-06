package ted.driver;

import ted.driver.sys.TedDriverImpl;

/**
 * @author Augustus
 * created on 2018.08.17
 *
 * For ted-scheduler only!
 */
public abstract class _SchdExt {
    protected TedDriverImpl tedDriverImpl;
    public _SchdExt(TedDriver tedDriver) {
        this.tedDriverImpl = tedDriver.tedDriverImpl;
    }
}
