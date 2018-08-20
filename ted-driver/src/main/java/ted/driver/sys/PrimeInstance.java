package ted.driver.sys;

import ted.driver.Ted.PrimeChangeEvent;
import ted.driver.sys.TedDriverImpl.TedContext;
import ted.driver.sys.TedDaoAbstract.DbType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Augustus
 *         created on 2018.07.28
 *
 * for TED internal usage only!!!
 *
 * for checking for being "prime" - ensure
 * - there can be only 1 prime at one time
 * - after prime dies, any other instance will become the prime
 *
 */
public final class PrimeInstance {
	private final static Logger logger = LoggerFactory.getLogger(PrimeInstance.class);
	private final static Logger loggerConfig = LoggerFactory.getLogger("ted-config");
	final static int TICK_SKIP_COUNT = 3; // every 3 check periods

	private final TedContext context;

	private boolean enabled = false;
	private boolean initiated = false;
	private Long primeTaskId = null;
	private int postponeSec = 3;
	private boolean isPrime = false;

	private PrimeChangeEvent onBecomePrime = null;
	private PrimeChangeEvent onLostPrime = null;

	final CheckPrimeParams checkPrimeParams = new CheckPrimeParams() {
		public boolean isPrime() { return isPrime; };
		public String instanceId() { return context.config.instanceId(); };
		public long primeTaskId() { return primeTaskId; }
		public int postponeSec() { return postponeSec; };

	};

	boolean isEnabled() {
		return enabled;
	}

	public PrimeInstance(TedContext context) {
		this.context = context;
	}

	public void enable() {
		if (context.tedDao.getDbType() != DbType.POSTGRES)
			throw new IllegalStateException("Prime instance feature is allowed for PostgreSQL db yet. TODO");
		enabled = true;
		//if (initiated)
		init(); // re-init
	}

	// after configs read
	void init() {
		if (! isEnabled()) {
			loggerConfig.info("Ted prime instance check is disabled");
			return;
		}
		this.primeTaskId = context.tedDao.findPrimeTaskId();

		int periodMs = context.config.intervalDriverMs();
		this.postponeSec = (int)Math.round((1.0 * periodMs * TICK_SKIP_COUNT + 500 + 500) / 1000); // 500ms reserve, 500 for rounding up

		becomePrime();

		loggerConfig.info("Ted prime instance check is enabled, primeTaskId={} isPrime={} postponeSec={}", primeTaskId, isPrime, postponeSec);
		initiated = true;
	}

	void becomePrime() {
		if (isPrime)
			return;
		this.isPrime = context.tedDao.becomePrime(primeTaskId, context.config.instanceId());
		if (isPrime) {
			logger.info("TED become prime. instanceId={}", context.config.instanceId());
			if (onBecomePrime != null) {
				ThreadPoolExecutor workers = context.registry.getChannel(Model.CHANNEL_SYSTEM).workers;
				workers.execute(new Runnable() {
					@Override
					public void run() {
						try {
							onBecomePrime.onEvent();
						} catch (Exception e) {
							logger.error("Exception onBecomePrime handler", e);
						}
					}
				});
			}
		}
	}

	void lostPrime() {
		if (! isPrime)
			return;
		logger.info("TED lost prime. instanceId={}", context.config.instanceId());
		this.isPrime = false;
		if (onLostPrime != null) {
			ThreadPoolExecutor workers = context.registry.getChannel(Model.CHANNEL_SYSTEM).workers;
			workers.execute(new Runnable() {
				@Override
				public void run() {
					try {
						onLostPrime.onEvent();
					} catch (Exception e) {
						logger.error("Exception onLostPrime handler", e);
					}
				}
			});
		}
	}

	public boolean isPrime() {
		return isPrime;
	}

	Long primeTaskId() {
		return primeTaskId;
	}

	interface CheckPrimeParams {
		boolean isPrime();
		String instanceId();
		long primeTaskId();
		int postponeSec();
	}

	public void setOnBecomePrime(PrimeChangeEvent onBecomePrime) {
		this.onBecomePrime = onBecomePrime;
	}

	public void setOnLostPrime(PrimeChangeEvent onLostPrime) {
		this.onLostPrime = onLostPrime;
	}
}
