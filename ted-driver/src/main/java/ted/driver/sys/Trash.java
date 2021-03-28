package ted.driver.sys;

import ted.driver.Ted.TedStatus;

// for something still disabled
class Trash {
    /**
     * metrics functions. will be called from ted-driver. can be used to measure ted work parameters.
     */
    public interface TedMetricsEvents {
        void dbCall(String logId, int resultCount, int durationMs);

        void loadTask(long taskId, String taskName, String channel);
        void startTask(long taskId, String taskName, String channel);
        void finishTask(long taskId, String taskName, String channel, TedStatus status, int durationMs);
    }
}
