package ted.scheduler

import org.awaitility.Awaitility
import org.junit.After
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
import org.junit.Assert.assertTrue
import org.junit.Ignore
import ted.scheduler.TestUtils.sleepMs
import ted.scheduler.impl.DaoPostgres
import ted.scheduler.impl.TedSchedulerImpl
import ted.scheduler.utils.CronExpression
import java.util.concurrent.TimeUnit

class I01SimpleTest {
    private val logger = LoggerFactory.getLogger(I01SimpleTest::class.java)

    private lateinit var driver: TedDriver
    private lateinit var scheduler: TedScheduler
    private lateinit var schedulerImpl: TedSchedulerImpl
    private lateinit var dao: DaoPostgres
    private lateinit var systemId: String


    @Before
    @Throws(IOException::class)
    fun init() {
        val properties = TestUtils.readPropertiesFile("ted-I01.properties")
        driver = TedDriver(TestConfig.testDbType, TestConfig.getDataSource(), properties)
        systemId = properties.getProperty("ted.systemId")
        dao = DaoPostgres(TestConfig.getDataSource(), systemId)
        driver.enablePrime()
        driver.start()
        scheduler = TedScheduler(driver)
        schedulerImpl = TedSchedulerImpl(driver)
        //this.context = driver.getContext();

    }

    @After
    fun cleanup() {
        driver.shutdown()
        dao_execSql("update tedtask set nextts = null, status = 'DONE' where system = '$systemId' and "
                + " status in ('NEW', 'RETRY', 'WORK') returning taskid")
    }

    @Ignore
    @Test
    fun test1() {
        val cron = CronExpression("0/11 14/19 0 1,11,21 1/2 *")
        var zdt = cron.nextTimeAfter(ZonedDateTime.now())
        zdt = cron.nextTimeAfter(zdt); println(zdt)
        zdt = cron.nextTimeAfter(zdt); println(zdt)
        zdt = cron.nextTimeAfter(zdt); println(zdt)
    }

    @Test
    fun testSchd01() {
        var count = 0
        scheduler.builder().name("TEST1")
                .scheduleCron("* * * ? * *")
                .runnable {
                    count++;
                    logger.info("executing scheduler task $count")
                }
                .register()
        Awaitility.await().atMost(2500, TimeUnit.MILLISECONDS).until { count >= 2 }
    }

    @Test
    fun testSchd02_should_be_only_1_task_with_same_name() {
        var count = 0
        var count2 = 0
        val shdId = scheduler.builder().name("TEST2")
                .scheduleCron("* * * ? * *")
                .runnable {
                    count++;
                    logger.info("[1] executing scheduler task $count")
                }
                .register()

        val shdId2 = scheduler.builder().name("TEST2")
                .scheduleCron("* * * ? * *")
                .runnable {
                    count2++;
                    logger.info("[2] executing scheduler task $count2")
                }
                .register()

        assertEquals(shdId, shdId2)

        // only 1st registration is active

        Awaitility.await().atMost(2500, TimeUnit.MILLISECONDS).until { count >= 2 }

        assertEquals(0, count2)
    }

    @Test
    fun testMaint() {
        val taskId = schedulerImpl.registerScheduler("TEST1", null,
                Factory.single(Runnable { logger.info("executing scheduler task") }),
                CronRetry("0 0/10 * ? * *"))
        print("scheduler taskId=$taskId")
        sleepMs(20)

        var task = driver.getTask(taskId)!!
        assertTrue(setOf(TedStatus.RETRY, TedStatus.NEW).contains(task.status))

        sleepMs(20)
        dao_execSql("update tedtask set status = 'ERROR', nextts = null where taskid = $taskId returning taskid")
        sleepMs(20)

        task = driver.getTask(taskId)
        assertEquals(TedStatus.ERROR, task.status)
        schedulerImpl.checkForErrorStatus()

        sleepMs(20)

        task = driver.getTask(taskId)
        assertEquals(TedStatus.RETRY, task.status)

        sleepMs(2000)
    }

    private fun dao_execSql(sql: String) {
        dao.selectData("test", sql, Void::class, emptyList())
    }

    fun print(msg: String) {
        println(msg)
    }

}
