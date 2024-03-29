package ted.driver;

import ted.driver.Ted.TedStatus;

import java.util.Date;

/**
 * @author Augustus
 *         created on 2016.09.12
 *
 * TedTask object will be passed to processor.
 */
public interface TedTask {

    Long getTaskId();
    String getName();
    String getChannel();
    String getKey1();
    String getKey2();
    String getData();
    Integer getRetries();
    Date getCreateTs();
    Date getStartTs();
    Long getBatchId();
    TedStatus getStatus();

    /** is task executing first time */
    boolean isNew();
    /** is task executing not first time */
    boolean isRetry();
    /** is task after timout (was returned from status 'WORK') */
    boolean isAfterTimeout();

    /** is this last try for task (will not retry anymore); null - unknown (may not be set for retryScheduler; for regular taskProcessor should be set) */
    default Boolean isLastTry() {
        return null;
    }
}
