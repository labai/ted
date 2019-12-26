package ted.spring.conf;

/**
 * @author Augustus
 *         created on 2019.12.04
 *
 * Default Retry exception.
 * Task processor can throw this (or any extended) exception to
 * send task to RETRY
 *
 * As alternative, alowed retry exception can be provided
 * with @TedTaskProcessot retryException parameter
 *
 */
public class TedRetryException extends RuntimeException {
	public TedRetryException() {
		super();
	}

	public TedRetryException(String message) {
		super(message);
	}

	public TedRetryException(String message, Throwable cause) {
		super(message, cause);
	}

	public TedRetryException(Throwable cause) {
		super(cause);
	}

	protected TedRetryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
