package com.github.ted.sys;

import com.github.ted.Ted.TedRetryScheduler;
import com.github.ted.Ted.TedTask;
import com.github.ted.sys.TedDriverImpl.TedContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @author Augustus
 *         created on 2017.04.25
 *
 * for TED internal usage only!!!
 *
 * - PeriodPatternRetryScheduler
 * 		retry by pattern in config (e.g.: "2s,10s,30s*5,10m*3,1h*5;dispersion=10")
 * 		may use dispersion
 *
 */

class RetryConfig {

	private static final Random random = new Random();

	private final TedContext context;

	public RetryConfig(TedContext context) {
		this.context = context;
	}

	// retry scheduler by config pattern (configured periods)
	// pattern e.g.: "2s,10s,30s*5,10m*3,1h*5;dispersion=10"
	static class PeriodPatternRetryScheduler implements TedRetryScheduler {

		private PeriodPatternConfig periodPatternConfig;

		public PeriodPatternRetryScheduler(String periodPattern) {
			this.periodPatternConfig = new PeriodPatternConfig(periodPattern);
		}

		// return null if retry not possible, or next retry time
		@Override
		public Date getNextRetryTime(TedTask task, int retryNumber, Date startTime) {
			return periodPatternConfig.getNextRetry(retryNumber);
		}
	}

	//
	// private
	//
	static class PeriodPatternConfig {
		static class RetryPause {
			final int timeSec;
			final int count;
			RetryPause(int timeSec, int count) {
				this.timeSec = timeSec;
				this.count = count;
			}
		}
		private final String pattern;
		private final List<RetryPause> pauses;
		private final int retryDispersion;

		public PeriodPatternConfig(String pattern) {
			if (pattern == null)
				throw new IllegalArgumentException("Pattern is null");
			this.pattern = pattern.replace(" ", "");
			try {
				this.retryDispersion = parsePeriodDispersion(this.pattern);
				this.pauses = parsePeriodPattern(this.pattern);
			} catch (Exception e) {
				throw new RuntimeException("Cannot parse parse retry pattern '" + pattern + "'", e);
			}

		}

		// returns null if retry not allowed
		Integer getNextRetryPauseSec(int retryNum){
			if (retryNum < 1)
				throw new IllegalArgumentException("retryNum must be >= 1");
			retryNum--; // use 0-base
			int counter = 0;
			int ipos = 0;
			do {
				RetryPause rp = pauses.get(ipos);
				if (retryNum >= counter && retryNum < counter + rp.count)
					return rp.timeSec;
				counter += rp.count;
				ipos++;
			} while (ipos < pauses.size() );
			return null;
		}

		Date getNextRetry(int retryNum) {
			Integer waitSec = getNextRetryPauseSec(retryNum);
			if (waitSec == null)
				return null;
			long waitMs = (waitSec * 1000);
			int dispersion = retryDispersion;
			if (dispersion != 0) // add some random distribution (90% - 110%)
				waitMs = waitMs * (100 - dispersion + random.nextInt(2 * dispersion + 1)) / 100;
			return new Date(System.currentTimeMillis() + waitMs) ;
		}

		static int parsePeriodDispersion(String pattern) {
			if (pattern == null)
				throw new IllegalArgumentException("Pattern is null");
			pattern = pattern.trim().replace(" ", "");
			if (pattern.isEmpty())
				throw new IllegalArgumentException("Pattern is empty");
			if (!pattern.contains(";dispersion="))
				return 0; // no config about dispersion - return 0

			String dispersionPattern = (pattern + ";").split(";")[1];
			dispersionPattern = dispersionPattern.substring("dispersion=".length());
			return Integer.parseInt(dispersionPattern);
		}

		// pauses=2s,10s,30s*5,10m*3,1h*5;dispersion=10
		private static List<RetryPause> parsePeriodPattern(String pattern) {
			if (pattern == null)
				throw new IllegalArgumentException("Pattern is null");
			pattern = pattern.trim().replace(" ", "");
			if (pattern.isEmpty())
				throw new IllegalArgumentException("Pattern is empty");
			String pausePattern = (pattern + ";").split(";")[0];

			List<RetryPause> retryPauses = new ArrayList<RetryPause>();
			String[] paus = pausePattern.split(",");
			for (String p : paus) {
				String[] pair = p.split("\\*");
				int count = 1;
				String stime = pair[0];
				if (pair.length == 2)
					count = Integer.parseInt(pair[1]);
				if (count < 1 || count > 1000000000)
					throw new RuntimeException("Count must be between 1 and 1000000000");

				Character suffix = stime.charAt(stime.length() - 1);
				if (!Arrays.asList('s', 'm', 'h').contains(suffix))
					throw new RuntimeException("Timer part must contain interval type (s, m, or h)");
				stime = stime.substring(0, stime.length() - 1);
				int timeSec = Integer.parseInt(stime);
				switch (suffix) {
					case 's': break;
					case 'm': timeSec = timeSec * 60; break;
					case 'h': timeSec = timeSec * 3600; break;
					default: throw new RuntimeException("Timer part must contain interval type (s, m, or h)");
				}
				retryPauses.add(new RetryPause(timeSec, count));
			}
			return retryPauses;
		}
	}

}

