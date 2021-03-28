package ted.scheduler

import java.io.FileNotFoundException
import java.io.IOException
import java.io.InputStream
import java.text.MessageFormat
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Properties

/**
 * @author Augustus
 * created on 2016.09.20
 */
internal object TestUtils {

    @Throws(IOException::class)
    fun readPropertiesFile(propFileName: String): Properties {
        val properties = Properties()
        val inputStream = TestBase::class.java.getClassLoader().getResourceAsStream(propFileName)
            ?: throw FileNotFoundException("property file '$propFileName' not found in the classpath")
        properties.load(inputStream)
        return properties
    }

    fun sleepMs(ms: Int) {
        try {
            Thread.sleep(ms.toLong())
        } catch (e: InterruptedException) {
            throw RuntimeException("Cannot sleep", e)
        }

    }


    fun log(msg: String) {
        println(SimpleDateFormat("mm:ss.SSS").format(Date()) + " " + msg)
    }

    fun log(pattern: String, vararg args: Any) {
        val msg = MessageFormat.format(pattern, *args)
        log(msg)
    }

    fun print(msg: String) {
        println(msg)
    }

}
