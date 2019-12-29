# Start to use

### Preparation

- Need to have db (Postgre 9.6+, MySql 8+ or Oracle)
- Create tedtask tables and indexes 
(see [ted_db_struct_pgre.sql](/labai/ted/blob/master/docs/init/ted_db_struct_pgre.sql) 
or [mysql](/labai/ted/blob/master/docs/init/ted_db_struct_mysql.sql) 
or [oracle](/labai/ted/blob/master/docs/init/ted_db_struct_ora.sql) 
in (ted)/docs/init/))

#### Use ted-driver
- Take latest ted-driver from maven repository 
```xml
<dependency>
   <groupId>com.github.labai</groupId>
   <artifactId>ted-driver</artifactId>
   <version>0.3.2</version>
</dependency>
```
- Configure DataSource in java
- Configure ted driver in java, register tasks processors
```java
@Configuration
public class TedConfig {

    @Bean
    public TedDriver tedDriver(){
        Properties properties = new Properties(); 
        properties.load(getClass().getResourceAsStream("ted.properties"));
        
        TedDriver tedDriver = new TedDriver(dataSource, properties);

        // register factories, which returns TedProcessor object
        tedDriver.registerTaskConfig("DATA_SYN", s -> tedJobs::syncData);

        tedDriver.start();

        return tedDriver;
    }        
}
```

- write task processor logic
```java
@Component
public class TedJobs {

    public TedResult syncData (TedTask task) {
        if (isEmpty(task.getData()))
            return TedResult.error("task.data is empty");

        logger.info("start DATA_SYN: {}", task.getData());

        return TedResult.done();
    }

}
```

- create tasks for processing
```java
    // create TedTask
    tedDriver.createTask("DATA_SYN", "{\"customerId\" : \"1234\"}");
```

- Prepare ted.properties file. This file allows to configure tasks externally. 
See example in [ted-sample.properties](/labai/ted/blob/master/ted-driver/src/test/resources/ted-sample.properties) 

For Spring also see [[Start to use in spring]] 

## Samples

There are several samples:
- [ted-samples/ted-sample1](/labai/ted/tree/master/ted-samples/ted-sample1) - few simple examples how TED can be used
- [ted-samples/ted-sample3](/labai/ted/tree/master/ted-samples/ted-sample3) - sample with SpringBoot, use AbstractTedProcessor 
 
