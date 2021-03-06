package sample1;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.TedDriver;
import ted.driver.TedResult;
import ted.driver.TedTask;
import ted.driver.TedTaskManager;
import ted.driver.task.TedBatchFactory.BatchBuilder;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Augustus
 *         created on 2018.08.01
 */
public class Sample1_4_batch {
    private static final Logger logger = LoggerFactory.getLogger(Sample1_4_batch.class);

    private static final String TASK_NAME = "PROCESS_LINE";
    private static final String BATCH_TASK = "FINISH_PROCESS";

    // connection to db configuration
    //
    private static DataSource dataSource() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName("org.hsqldb.jdbc.JDBCDriver");
        dataSource.setJdbcUrl("jdbc:hsqldb:mem:tedtest;sql.syntax_pgs=true");
//		dataSource.setDriverClassName("org.postgresql.Driver");
//		dataSource.setJdbcUrl("jdbc:postgresql://localhost:5433/ted");
        dataSource.setUsername("ted");
        dataSource.setPassword("ted");

        // init in-memo db
        if ("org.hsqldb.jdbc.JDBCDriver".equals(dataSource.getDriverClassName())) {
            executeInitScript("schema-hsqldb.sql", dataSource);
        }

        return dataSource;
    }

    private static TedDriver tedDriver() {
        Properties properties = new Properties();
        String propFileName = "ted.properties";
        InputStream inputStream = Sample1_4_batch.class.getClassLoader().getResourceAsStream(propFileName);
        if (inputStream == null)
            throw new RuntimeException("Property file '" + propFileName + "' not found in the classpath");
        try {
            properties.load(inputStream);
            inputStream.close();
        } catch (IOException e) {
            throw new RuntimeException("Cannot read property file '" + propFileName + "'", e);
        }
        DataSource dataSource = dataSource();
        TedDriver tedDriver = new TedDriver(dataSource, properties);
        return tedDriver;


    }

    public static void main(String ... args) throws IOException {
        String fileName = "input.txt";
        System.out.println("start sample1");

        // init ted, register tasks
        //
        TedDriver tedDriver = tedDriver();
        tedDriver.registerTaskConfig(TASK_NAME, s -> Sample1_4_batch::processLine) ;
        tedDriver.registerTaskConfig(BATCH_TASK, s -> Sample1_4_batch::processBatch) ;
        tedDriver.start();

        TedTaskManager taskHelper = new TedTaskManager(tedDriver);

        // read some big file for processing
        //
        File file = new File(Sample1_4_batch.class.getClassLoader().getResource(fileName).getPath());
        List<String> lines = FileUtils.readLines(file, "UTF-8");

        // create tasks for each line
        //
        List<TedTask> tasks = new ArrayList<>();

        BatchBuilder bbuilder = taskHelper.getBatchFactory().batchBuilder(BATCH_TASK);
        for (String line : lines) {
            bbuilder.addTask(TASK_NAME, line, null, null);
        }

//		Long batchId = tedDriver.createBatch(BATCH_TASK, "batch task data", null, null, tasks);
        Long batchId = bbuilder.create();

        // wait a while, while ted will process tasks. see processing info in logs
        //
        sleep(8000);

        tedDriver.shutdown();
        System.out.println("finish sample1_4_batch");
    }

    // file line processor
    //
    private static TedResult processLine(TedTask task) {
        if (isEmpty(task.getData()))
            return TedResult.error("task.data is empty");
        int sleepMs = RandomUtils.nextInt(200, 900);
        System.out.println("PROCESS LINE " + task.getData());
        logger.info("do something smart with line: '{}' for {}ms", task.getData(), sleepMs);
        sleep(sleepMs);
        return TedResult.done();
    }

    //
    //
    private static TedResult processBatch(TedTask task) {
        logger.info("all tasks finished");
        return TedResult.done();
    }

    private static void sleep(long milis) {
        try {
            Thread.sleep(milis);
        } catch (InterruptedException e2) {
        }
    }

    private static void executeInitScript(String scriptFile, DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()){
            InputStream inputStream = Sample1_4_batch.class.getClassLoader().getResourceAsStream(scriptFile);
            SqlFile sqlFile = new SqlFile(new InputStreamReader(inputStream), "init", System.out, "UTF-8", false, new File("."));
            sqlFile.setConnection(connection);
            sqlFile.execute();
        } catch (SQLException | SqlToolError | IOException e) {
            e.printStackTrace();
        }
    }

}
