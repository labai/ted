package ted.driver;

import ted.driver.sys.TedDriverImpl;
import ted.driver.task.TedBatchFactory;
import ted.driver.task.TedTaskFactory;

/**
 * @author Augustus
 * created on 2019.11.26
 *
 * Helper class to create tasks
 *
 * Can be initiated in two ways:
 * - using constructor with provided TedDriver
 * - using no-arg constructor and setter setTedDriver
 *
 * second one can be used in case when need to break circural dependency
 *
 */
public class TedTaskManager {

    private TedDriverImpl tedDriverImpl; // for internal, not for in apps

    private final TedTaskFactory taskFactory;
    private final TedBatchFactory batchFactory;

    public TedTaskManager(TedDriver tedDriver) {
        this.tedDriverImpl = tedDriver.tedDriverImpl;
        this.taskFactory = new TedTaskFactory(tedDriverImpl);
        this.batchFactory = new TedBatchFactory(tedDriverImpl);
    }

    public TedTaskManager() {
        this.taskFactory = new TedTaskFactory(() -> tedDriverImpl);
        this.batchFactory = new TedBatchFactory(() -> tedDriverImpl);
    }

    public void setTedDriver(TedDriver tedDriver) {
        this.tedDriverImpl = tedDriver.tedDriverImpl;
    }

    public TedTaskFactory getTaskFactory() {
        return taskFactory;
    }

    public TedBatchFactory getBatchFactory() {
        return batchFactory;
    }
}
