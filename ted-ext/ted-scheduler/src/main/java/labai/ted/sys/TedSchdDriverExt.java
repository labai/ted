package labai.ted.sys;


import labai.ted.TedDriver;
import labai.ted.TedSchdHck1;

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
public class TedSchdDriverExt {
	private final TedDriver tedDriver;
	private final TedDriverImpl tedDriverImpl;

	public TedSchdDriverExt(TedDriver tedDriver) {
		this.tedDriver = tedDriver;
		this.tedDriverImpl = TedSchdHck1.getTedDriverImpl(tedDriver);
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
