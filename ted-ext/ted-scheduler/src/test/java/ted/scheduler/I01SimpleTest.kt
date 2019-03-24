package ted.scheduler

import org.junit.Before
import org.junit.Test
import org.slf4j.LoggerFactory
import ted.driver.Ted.TedStatus
import ted.driver.TedDriver
import ted.scheduler.impl.TedSchedulerImpl.CronRetry
import ted.scheduler.impl.TedSchedulerImpl.Factory

import java.io.IOException
import java.time.ZonedDateTime

import org.junit.Assert.assertEquals
import org.junit.Ignore
import ted.scheduler.TestUtils.sleepMs
import ted.scheduler.impl.DaoPostgres
import ted.scheduler.impl.TedSchedulerImpl
import ted.scheduler.utils.CronExpression

class I01SimpleTest {
    private val logger = LoggerFactory.getLogger(I01SimpleTest::class.java)

    private lateinit var driver: TedDriver
    private lateinit var scheduler: TedScheduler
    private lateinit var schedulerImpl: TedSchedulerImpl
    private lateinit var dao: DaoPostgres


    @Before
    @Throws(IOException::class)
    fun init() {
        val properties = TestUtils.readPropertiesFile("ted-I01.properties")
        driver = TedDriver(TestConfig.testDbType, TestConfig.getDataSource(), properties)
        dao = DaoPostgres(TestConfig.getDataSource(), properties.getProperty("ted.systemId"))
        driver.enablePrime()
        driver.start()
        scheduler = TedScheduler(driver)
        schedulerImpl = TedSchedulerImpl(driver)
        //this.context = driver.getContext();

    }

    @Ignore
    @Test
    fun test1() {
        val cron = CronExpression("0/11 14/19 0 1,11,21 1/2 *")
        var zdt = cron.nextTimeAfter(ZonedDateTime.now())
        zdt = cron.nextTimeAfter(zdt); print(zdt)
        zdt = cron.nextTimeAfter(zdt); print(zdt)
        zdt = cron.nextTimeAfter(zdt); print(zdt)
    }

    @Ignore
    @Test
    fun testSchd01() {
        scheduler.builder().name("TEST1")
                .scheduleCron("0 0/10 * 1/1 * *")
                .runnable(Runnable { logger.info("executing scheduler task") })
                .register()
        sleepMs(2000)
    }

    @Test
    fun testMaint() {
        val taskId = schedulerImpl.registerScheduler("TEST1", null,
                Factory.single(Runnable { logger.info("executing scheduler task") }),
                CronRetry("0 0/10 * ? * *"))
        print("scheduler taskId=$taskId")
        var task = driver.getTask(taskId)
        assertEquals(TedStatus.RETRY, task!!.status)
        sleepMs(20)
        dao_execSql("update tedtask set status = 'ERROR', nextts = null where taskid = $taskId returning taskid")
        sleepMs(20)
        task = driver.getTask(taskId)
        assertEquals(TedStatus.ERROR, task!!.status)
        schedulerImpl.checkForErrorStatus()
        sleepMs(20)
        task = driver.getTask(taskId)
        assertEquals(TedStatus.RETRY, task!!.status)
        sleepMs(2000)
    }

    private fun dao_execSql(sql: String) {
        dao.selectData<Void>("test", sql, Void::class.java, emptyList())
    }

    fun print(msg: String) {
        println(msg)
    }

}
