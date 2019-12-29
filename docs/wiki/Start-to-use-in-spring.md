# Start to use in spring

`ted-spring-support` helps to easily integrated ted-driver into Spring.  

### Preparation

- Need to have db (Postgre 9.6+, MySql 8+ or Oracle)
- Create tedtask tables and indexes 
(see [ted_db_struct_pgre.sql](/labai/ted/blob/master/docs/init/ted_db_struct_pgre.sql) 
or [mysql](/labai/ted/blob/master/docs/init/ted_db_struct_mysql.sql) 
or [oracle](/labai/ted/blob/master/docs/init/ted_db_struct_ora.sql) 
in (ted)/docs/init/))

#### Use ted-driver
- Take latest `ted-spring-support` from maven repository (it will contain required dependencies to ted-driver) 
```xml
<dependency>
   <groupId>com.github.labai</groupId>
   <artifactId>ted-spring-support</artifactId>
   <version>0.3.2</version>
</dependency>
```
- Configure DataSource
- Add `@EnableTedTask` annotation and `@TedTaskProcessor` for task processors 
E.g.
```java
@EnableTedTask
@Configuration
public class TedConfig {
    
    @TedTaskProcessor(name = "TASK1")
    public TedResult task1(TedTask task) {
        logger.info("start TASK1: {}", task.getData());
        return TedResult.done();
    }
    
    // scheduler task
    @TedSchedulerProcessor(name = "SCH_1", cron = "1 * * * * *")
    public String schedulerTask1() {
        logger.info("Start schedulerTask1");
        return "ok";
    }
}
```
- Add TED configuration to application.properties file. 
See example in [ted-sample.properties](/labai/ted/blob/master/ted-driver/src/test/resources/ted-sample.properties) 

## Samples

There are several samples with spring configuration:
- [ted-samples/ted-sample4](/labai/ted/tree/master/ted-samples/ted-sample4) - example how to configure ted with spring
- [ted-samples/ted-sample5](/labai/ted/tree/master/ted-samples/ted-sample5) - example how to choose DataSource when few of them are in project  
 
