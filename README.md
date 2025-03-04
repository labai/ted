# TedLib - task execution lib

## About

TED is library for Java to handle task execution.

Tasks are stored in PostgreSql/Oracle db (table _tedtask_). 
Ted checks for new tasks in that table, retrieves them, calls task processor and after finish sets status (DONE, ERROR or RETRY).

##### Main things kept in mind while creating TED
- simplicity of use;
- good performance;
- no jar dependencies;
- easy support using standard sql tools (view task history, restart task);
- can be used for short (milliseconds) and long (hours) tasks;

##### Features
Main features:
- is a part of war (or other app), no needs for separate process;
- tasks are in db, thus tasks remain after restart of app, it is easy to browse history ann manage using standard sql;  
- there is _auto cleaning_ of tasks;
- task is executed only by one application instance;
- works with PostgreSQL (9.5+), Oracle DB; from Java 1.8;
- _task configuration_ is in separate ted.properties file, may be tuned without recompile;
- _channels_ - allows to assign separate thread pools to different types of tasks; 
- [ted-scheduler](ted-ext/ted-scheduler/readme.md) allows using ted-driver as scheduler engine

See more in [Ted features](docs/wiki/Ted-features.md) in wiki.

 
## Usage example

```xml
<dependency>
    <groupId>com.github.labai</groupId>
    <artifactId>ted-driver</artifactId>
    <version>0.3.8</version>
</dependency>
```

```java
@Configuration
public class TedConfig {
  ...
  @Bean
  public TedDriver tedDriver(){
    ... 
    TedDriver tedDriver = new TedDriver(TedDbType.POSTGRES, dataSource, properties);
    // register factories, which returns TedProcessor object
    tedDriver.registerTaskConfig("DATA_SYN", taskName -> tedJobs.syncDataTedProcessor());
    tedDriver.start();
    return tedDriver;
  }
```

```java
// implements TedProcessor object
@Component
public class TedJobs {
    ...
    public TedProcessor syncInstrTedProcessor() {
        return task -> {
            if (isEmpty(task.getData()))
                return TedResult.error("task.data is empty");
            ...
            return TedResult.done();
        };
    }
}
```

and then create tasks 

```java
// create TedTask
tedDriver.createTask("DATA_SYN", "{\"customerId\" : \"1234\"}");
```

See also [Start-to-use](docs/wiki/Start-to-use.md)  
in wiki. More samples in [ted-samples](/labai/ted/tree/master/ted-samples).


## DB Structure

#### tedtask table

All tasks are stored into _tedtask_ table. Ted driver periodically checks for new tasks and takes portion for execution.  
