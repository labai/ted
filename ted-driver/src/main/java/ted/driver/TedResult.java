package ted.driver;

import ted.driver.Ted.TedStatus;

/**
 * @author Augustus
 *         created on 2016.09.12
 *
 * for passing processing result to Ted.
 *
 * E.g.
 *   ... return TedResult.done()
 * or
 * 	 ... return TedResult.retry("Record was locked")
 */
public class TedResult {
	private static final TedResult RES_DONE = new TedResult(TedStatus.DONE, null);
	private static final TedResult RES_RETRY = new TedResult(TedStatus.RETRY, null);
	private static final TedResult RES_ERROR = new TedResult(TedStatus.ERROR, null);
	private final TedStatus status;
	private final String message;

	private TedResult(TedStatus status, String message) {
		this.status = status;
		this.message = message;
	}

	public static TedResult error(String msg) {
		return new TedResult(TedStatus.ERROR, msg);
	}
	public static TedResult error() {
		return RES_ERROR;
	}

	public static TedResult done(String msg) {
		return new TedResult(TedStatus.DONE, msg);
	}

	public static TedResult done() {
		return RES_DONE;
	}

	public static TedResult retry(String msg) {
		return new TedResult(TedStatus.RETRY, msg);
	}

	public static TedResult retry() {
		return RES_RETRY;
	}

	public TedStatus status() {
		return status;
	}

	public String message() {
		return message;
	}
}
