package ted.driver;

import ted.driver.Ted.PrimeChangeEvent;
import ted.driver.Ted.TedDbType;
import ted.driver.Ted.TedProcessorFactory;
import ted.driver.Ted.TedRetryScheduler;
import ted.driver.TedDriverApi.TedDriverConfig;
import ted.driver.TedDriverApi.TedDriverConfigAware;
import ted.driver.TedDriverApi.TedDriverEvent;
import ted.driver.TedDriverApi.TedDriverNotification;
import ted.driver.TedDriverApi.TedDriverPrime;
import ted.driver.TedDriverApi.TedDriverService;
import ted.driver.TedDriverApi.TedDriverTask;
import ted.driver.TedDriverApi.TedDriverTaskConfig;
import ted.driver.sys.TedDriverImpl;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author Augustus
 *         created on 2016.09.12
 *
 *  TedDriver with api
 *
 */
public class TedDriver implements
    TedDriverService,
    TedDriverConfigAware,
    TedDriverTaskConfig,
    TedDriverTask,
//		TedDriverBatch,
    TedDriverEvent,
    TedDriverNotification,
    TedDriverPrime {

    TedDriverImpl tedDriverImpl; // for Ted-ext, do not use in app
    private TedDbType dbType;
    private DataSource dataSource;
    private TedDriverConfig driverConfig; /* some info about driver configuration */

    private boolean isStartedInitialize = false;


    /**
     *  dataSource - provides oracle db (with tedtask table) connection dataSource;
     *  properties - ted configuration
     */
    public TedDriver(TedDbType dbType, DataSource dataSource, Properties properties) {
        this.tedDriverImpl = new TedDriverImpl(dbType, dataSource, properties);
        this.dataSource = dataSource;
        this.dbType = dbType;
        this.driverConfig = tedDriverImpl.getTedDriverConfig();
        this.isStartedInitialize = true;
    }

    public TedDriver(DataSource dataSource, Properties properties) {
        // will connect to db to recognize its type
        this(resolveDbType(dataSource), dataSource, properties);
    }

    private static TedDbType resolveDbType(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection()) {
            String sdbtype = conn.getMetaData().getDatabaseProductName();
            switch (sdbtype) {
                case "PostgreSQL": return TedDbType.POSTGRES;
                case "Oracle": return TedDbType.ORACLE;
                case "MySQL": return TedDbType.MYSQL;
                case "HSQL Database Engine": return TedDbType.HSQLDB;
                default: throw new IllegalStateException("Cannot recognize db type or it is not supported by TED ("+ sdbtype +")");
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Connection error while trying to recognize db type", e);
        }
    }

    public TedDriver() {
    }

    public void init(TedDbType dbType, DataSource dataSource, Properties properties) {
        this.tedDriverImpl = new TedDriverImpl(dbType, dataSource, properties);
        this.dataSource = dataSource;
        this.dbType = dbType;
        this.driverConfig = tedDriverImpl.getTedDriverConfig();
        this.isStartedInitialize = true;
    }


    /**
     * start TED task manager
     */
    @Override
    public void start() {
        tedDriverImpl.start();
    }

    /**
     * shutdown TED
     */
    @Override
    public void shutdown() {
        tedDriverImpl.shutdown(20_000L);
    }

    /**
     * create task (to perform)
     */
    @Override
    public Long createTask(String taskName, String data, String key1, String key2) {
        return tedDriverImpl.createTask(taskName, data, key1, key2, null, null);
    }

    /**
     * create task - simple version
     */
    @Override
    public Long createTask(String taskName, String data) {
        return tedDriverImpl.createTask(taskName, data, null, null, null, null);
    }

    /**
     * create task for future execution (postponed)
     */
    @Override
    public Long createTaskPostponed(String taskName, String data, String key1, String key2, int postponeSec) {
        return tedDriverImpl.createTaskPostponed(taskName, data, key1, key2, postponeSec, null);
    }

    /**
     * create task and immediately execute it (will wait until execution finish)
     */
    @Override
    public Long createAndExecuteTask(String taskName, String data, String key1, String key2) {
        return tedDriverImpl.createAndExecuteTask(taskName, data, key1, key2, false, null);
    }

    /**
     * create task and start to process it in channel (will NOT wait until execution finish)
     */
    @Override
    public Long createAndStartTask(String taskName, String data, String key1, String key2) {
        return tedDriverImpl.createAndExecuteTask(taskName, data, key1, key2, true, null);
    }

    /**
     * create tasks by list and batch task for them. return batch taskId
     */
// moved to TedTaskManager
//	@Override
//	public Long createBatch(String batchTaskName, String data, String key1, String key2, List<TedTask> tedTasks) {
//		return tedDriverImpl.createBatch(batchTaskName, data, key1, key2, tedTasks);
//	}

    /**
     * create event in queue
     */
    @Override
    public Long createEvent(String taskName, String queueId, String data, String key2) {
        return tedDriverImpl.createEvent(taskName, queueId, data, key2);
    }

    /**
     * create event in queue. If possible, try to execute
     */
    @Override
    public Long createEventAndTryExecute(String taskName, String queueId, String data, String key2) {
        return tedDriverImpl.createEventAndTryExecute(taskName, queueId, data, key2);
    }

    /**
     * send notification to instances
     */
    @Override
    public Long sendNotification(String taskName, String data) {
        return tedDriverImpl.sendNotification(taskName, data);
    }

    /**
     * register task (configuration)
     */
    @Override
    public void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory) {
        tedDriverImpl.registerTaskConfig(taskName, tedProcessorFactory);
    }

    /**
     * register task (configuration) with own retryScheduler
     */
    @Override
    public void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory, TedRetryScheduler retryScheduler) {
        tedDriverImpl.registerTaskConfig(taskName, tedProcessorFactory, null, retryScheduler, null);
    }

    /**
     * get task by taskId (for current system only). Returns null if not found
     */
    @Override
    public TedTask getTask(Long taskId) {
        if (taskId == null)
            return null;
        return tedDriverImpl.getTask(taskId);
    }

    /**
     * get some info about driver configuration
     */
    @Override
    public TedDriverConfig getDriverConfig() {
        return driverConfig;
    }

    //
    // prime instance (for postgres)
    //

    /**
     * enable Check Prime functionality
     */
    @Override
    public void enablePrime() {
        tedDriverImpl.prime().enable();
    }

    /**
     * check, is current instance prime
     */
    @Override
    public boolean isPrime() {
        return tedDriverImpl.prime().isPrime();
    }

    /**
     * event listener on becoming prime
     */
    @Override
    public void setOnBecomePrimeHandler(PrimeChangeEvent onBecomePrime) {
        tedDriverImpl.prime().setOnBecomePrime(onBecomePrime);
    }

    /**
     * event listener on losing prime
     */
    @Override
    public void setOnLostPrimeHandler(PrimeChangeEvent onLostPrime) {
        tedDriverImpl.prime().setOnLostPrime(onLostPrime);
    }


    /**
     * helper function to
     * create TedTask for createBatch (with required params only)
     */
//	public TedTask newTedTask(String taskName, String data, String key1, String key2) {
//		return tedDriverImpl.newTedTask(taskName, key1, key2, data);
//	}

}
