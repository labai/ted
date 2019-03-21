package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.sys.PrimeInstance.CheckPrimeParams;
import ted.driver.sys.Registry.Channel;
import ted.driver.sys.TedDriverImpl.TedContext;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Augustus
 *         created on 2018.07.27
 *
 * for TED internal usage only!!!
 *
 * quick check in db for tasks. Will be called every 0.7s
 * (config "ted.driver.intervalDriverMs")
 *
 * we are making assumptions that single call with few selects to db
 * is somehow faster than few separated calls.
 *
 */
class QuickCheck {
	private final static Logger logger = LoggerFactory.getLogger(QuickCheck.class);
	private final static int SKIP_CHANNEL_THRESHOLD_MS = 100;

	private final TedContext context;

	private long nextPrimeCheckTimeMs = 0;
	private long checkIteration = 0;
	private boolean skipNextChannelCheck = false;

	QuickCheck(TedContext context) {
		this.context = context;
	}

	static class CheckResult {
		String type; // CHAN - waiting channels, PRIM - prime check results
		String name;
		Date tillTs;

		public CheckResult() {
		}

		public CheckResult(String type, String name) {
			this.type = type;
			this.name = name;
		}
	}

	private static class ParsedResults {

		private final Set<String> channelResults = new HashSet<String>();
		private final List<CheckResult> primeResults = new ArrayList<CheckResult>();

		public ParsedResults(List<CheckResult> checkResList) {
			for (CheckResult cres : checkResList) {
				if ("PRIM".equals(cres.type))
					primeResults.add(cres);
				if ("CHAN".equals(cres.type)) {
					channelResults.add(cres.name);
				}
			}
		}

		public boolean needProcessTedQueue() {
			return channelResults.contains(Model.CHANNEL_QUEUE);
		}

		public boolean needProcessTedBatch() {
			return channelResults.contains(Model.CHANNEL_BATCH);
		}

		public boolean needProcessTedNotify() {
			return channelResults.contains(Model.CHANNEL_NOTIFY);
		}

		public Set<String> getTaskChannels() {
			Set<String> taskChannels = new HashSet<String>();
			for (String chan : channelResults) {
				if (Model.nonTaskChannels.contains(chan))
					continue;
				taskChannels.add(chan);
			}
			return taskChannels;
		}

		public boolean canPrime() {
			return existsPrime("CAN_PRIME");
		}

		public boolean lostPrime() {
			return existsPrime("LOST_PRIME");
		}

		public boolean isPrime() {
			return existsPrime("PRIME");
		}

		public Date nextPrimeCheck() {
			for (CheckResult cres : primeResults) {
				if ("NEXT_CHECK".equals(cres.name)) {
					return cres.tillTs;
				}
			}
			return null;
		}

		private boolean existsPrime(String primeCheck){
			for (CheckResult cres : primeResults) {
				if (primeCheck.equals(cres.name))
					return true;
			}
			return false;
		}
	}

	public void quickCheck() {
		// do need to check is instance prime
		CheckPrimeParams checkPrimeParams = getCheckPrimeParams();

		long startTs = System.currentTimeMillis();

		List<CheckResult> checkResList = callDao(checkPrimeParams);

		checkIteration++;

		long checkDurationMs = System.currentTimeMillis() - startTs;

		ParsedResults results = new ParsedResults(checkResList);

		handleResultSystemChannels(results);

		handleResultTaskChannels(results, checkDurationMs);

		handleResultPrime(results);

	}


	private CheckPrimeParams getCheckPrimeParams() {
		CheckPrimeParams checkPrimeParams = null;
		if (context.prime.isEnabled()) {
			boolean needCheckPrime = false;
			if (context.prime.isPrime()) {
				// update every 3 ticks
				needCheckPrime = (checkIteration % PrimeInstance.TICK_SKIP_COUNT == 0);
			} else {
				needCheckPrime = (nextPrimeCheckTimeMs <= System.currentTimeMillis());
			}
			if (needCheckPrime) {
				checkPrimeParams = context.prime.checkPrimeParams;
			}
		}
		return checkPrimeParams;
	}


	private List<CheckResult> callDao(CheckPrimeParams checkPrimeParams) {
		// channels
		List<CheckResult> checkResList;
		try {
			if (skipNextChannelCheck)
				logger.debug("skip channels quick check");

			checkResList = context.tedDao.quickCheck(checkPrimeParams, skipNextChannelCheck);

			if (skipNextChannelCheck) { // add all channels
				checkResList = new ArrayList<CheckResult>(checkResList);
				for (Channel chan : context.registry.getChannels()) {
					checkResList.add(new CheckResult("CHAN", chan.name));
				}
			}
		} catch (RuntimeException e) {
			if (context.prime.isEnabled()) {
				context.prime.lostPrime();
			}
			throw e;
		}

		return checkResList;
	}

	private void handleResultTaskChannels(ParsedResults results, long checkDurationMs) {
		Set<String> allTaskChannels = results.getTaskChannels();

		List<String> taskChannels = new ArrayList<String>();

		// if we are not in prime instance, then remove channel allowed to run only in for prime instance
		if (context.prime.isEnabled() && context.prime.isPrime() == false) {
			for (String chanName : allTaskChannels) {
				Channel chan = context.registry.getChannel(chanName);
				if (chan == null || chan.primeOnly)
					continue;
				taskChannels.add(chanName);
			}
		} else {
			taskChannels.addAll(allTaskChannels);
		}

		// process tasks
		//
		if (! taskChannels.isEmpty()) {
			boolean wasAnyChannelFull = context.taskManager.processChannelTasks(taskChannels);
			// in some cases there may be created a lot of new tasks, and simple quick check may take time. Then just skip this quick check and process all channels. 100ms is threshold.
			this.skipNextChannelCheck = wasAnyChannelFull && (checkDurationMs > SKIP_CHANNEL_THRESHOLD_MS || skipNextChannelCheck);
		}

	}

	private void handleResultSystemChannels(ParsedResults checkResults) {
		if (checkResults.needProcessTedQueue()) {
			context.eventQueueManager.processTedQueue();
		}
		if (checkResults.needProcessTedBatch()) {
			context.batchWaitManager.processBatchWaitTasks();
		}
		if (checkResults.needProcessTedNotify()) {
			context.notificationManager.processNotifications();
		}

	}

	private void handleResultPrime(ParsedResults checkResults) {
		if (! context.prime.isEnabled())
			return;

		Date tillTs = checkResults.nextPrimeCheck();
		if (tillTs != null) {
			nextPrimeCheckTimeMs = Math.min(System.currentTimeMillis() + 3000, tillTs.getTime());
		}

		if (context.prime.isPrime() && checkResults.lostPrime()) {
			context.prime.lostPrime();
		} else if (! context.prime.isPrime() && checkResults.canPrime()) {
			context.prime.becomePrime();
		}
	}

}
