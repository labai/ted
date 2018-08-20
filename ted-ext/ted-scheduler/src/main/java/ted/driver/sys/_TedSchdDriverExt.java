package ted.driver.sys;


import ted.driver.TedDriver;
import ted.driver._TedSchdHck;

import javax.sql.DataSource;

/**
 * @author Augustus
 *         created on 2018.08.17
 *
 *  use some of internal functions.
 *
 *  For ted-scheduler only!
 *
 */
public class _TedSchdDriverExt {
	private final TedDriver tedDriver;
	private final TedDriverImpl tedDriverImpl;

	public _TedSchdDriverExt(TedDriver tedDriver) {
		this.tedDriver = tedDriver;
		this.tedDriverImpl = _TedSchdHck.getTedDriverImpl(tedDriver);
	}

	public String systemId() {
		return tedDriverImpl.getContext().config.systemId();
	}
	public String instanceId() {
		return tedDriverImpl.getContext().config.instanceId();
	}
	public Long primeTaskId() {
		return tedDriverImpl.getContext().prime.primeTaskId();
	}
	public DataSource dataSource() {
		return tedDriverImpl.dataSource;
	}
	public boolean isPrimeEnabled() {
		return tedDriverImpl.prime().isEnabled();
	}
}
