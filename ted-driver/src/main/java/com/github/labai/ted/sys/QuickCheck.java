package com.github.labai.ted.sys;

import com.github.labai.ted.sys.PrimeInstance.CheckPrimeParams;
import com.github.labai.ted.sys.Registry.Channel;
import com.github.labai.ted.sys.TedDriverImpl.TedContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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

	private final TedContext context;

	private long nextPrimeCheckTimeMs = 0;
	private long checkIteration = 0;

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

	public void quickCheck() {
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
		List<CheckResult> checkResList;
		try {
			checkResList = context.tedDao.quickCheck(checkPrimeParams);
		} catch (RuntimeException e) {
			if (context.prime.isEnabled()) {
				context.prime.lostPrime();
			}
			throw e;
		}
		checkIteration++;

		// process tasks
		List<String> taskChannels = new ArrayList<String>();
		boolean needProcessTedQueue = false;
		for (CheckResult cres : checkResList) {
			if ("CHAN".equals(cres.type) == false)
				continue;
			// filter prime tasks for non-prime instances
			if (context.prime.isEnabled() && context.prime.isPrime() == false) {
				Channel chan = context.registry.getChannel(cres.name);
				if (chan == null || chan.primeOnly)
					continue;
			}
			if (Model.CHANNEL_QUEUE.equalsIgnoreCase(cres.name)) {
				needProcessTedQueue = true;
			} else {
				if (Model.nonTaskChannels.contains(cres.name) == false)
					taskChannels.add(cres.name);
			}
		}
		if (! taskChannels.isEmpty()) {
			context.taskManager.processChannelTasks(taskChannels);
		}
		if (needProcessTedQueue) {
			context.eventQueueManager.processTedQueue();
		}
		// process prime check results
		if (context.prime.isEnabled()) {
			boolean canPrime = false;
			boolean lostPrime = false;
			boolean isPrime = false;
			for (CheckResult cres : checkResList) {
				if ("PRIM".equals(cres.type) == false)
					continue;
				if ("CAN_PRIME".equals(cres.name)) {
					canPrime = true;
				} else if ("LOST_PRIME".equals(cres.name)) {
					lostPrime = true;
				} else if ("PRIME".equals(cres.name)) {
					isPrime = true;
				} else if ("NEXT_CHECK".equals(cres.name)) {
					if (cres.tillTs != null)
						nextPrimeCheckTimeMs = Math.min(System.currentTimeMillis() + 3000, cres.tillTs.getTime());
				}
			}
			if (context.prime.isPrime() && lostPrime) {
				context.prime.lostPrime();
			} else if (! context.prime.isPrime() && canPrime) {
				context.prime.becomePrime();
			}
		}
	}


}
