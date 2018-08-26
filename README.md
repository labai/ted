# TED - Task Execution Driver

## About

TED is library for Java to handle task execution.

Tasks are created in Oracle/Postgres db (table _tedtask_). 
TED will check for new tasks in that table, retrieves them, call task processor and after finish will set status (DONE, ERROR or RETRY).

##### Main things kept in mind while creating TED
- simplicity of use;
- good performance;
- no jar dependencies;
- easy support - it is possible to view task history in db, restart task;
- can be used for short (~50ms) and long (~5h) tasks;

##### Features
Main features:
- is a part of war (or other app), there are no needs for separate process;
- tasks are in db, thus tasks remain after restart of tomcat, it is easy to browse history;  
- there are _auto cleaning_ after 35 days;
- task will be executed only by one application instance (any);
- works with PostgreSQL (9.5+) or Oracle DB; from Java 1.6;
- _task configuration_ is in separate ted.properties file;
- it is possible to retry task by it's own _retry policy_;
- _channels_ - allows to assign separate thread pools to different tasks; 

Additionally features:
- _prime instance_ - managed prime (master) instance between few instances/nodes.
- _event queue_ - it is possible to execute tasks in strictly the same sequence, as they were created.

See [Ted features](docs/wiki/Ted-features.md) in wiki.
 
TED can be helpful when:
- you need to have task history in db table, easy browse, restart one or few tasks;
- to be sure task will remain even if app will be shutdown before finish execution;
- need to balance load between few instances (e.g. tomcats);
- need retry logic;

There can be many use cases of TED, just few examples:
- send (or wait for) asynchronous response to external system - task will remain in db even if tomcat will be shutdown, task will be retried until success;
- generate lot of (say 50k) reports for customers (just create 50k tasks);
- generate report by request asynchronously;
- maintenance task (e.g. for cleaning db data) - will ensure only one instance do this task;


### Comparision 

Difference from spring-batch
- spring-batch is designed for big batch processing, while TED approach is to divide big batch into independent small(er) tasks, which will have their own live cycle (e.g. instead of creating 50k reports in one batch, in TED scenario you will need to create 50k separate tasks). While it is possible to execute and long-running tasks in TED, but task's internal logic is not interesting for TED.

Difference from quartz
- TED is not designed as scheduler, but it is possible to use it as simple scheduler. 
See [ted-scheduler](ted-ext/ted-scheduler/readme.md).    
 
## Usage

```xml
<dependency>
   <groupId>com.github.labai</groupId>
   <artifactId>ted</artifactId>
   <version>0.2.0</version>
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
    tedDriver.registerTaskConfig("DATA_SYN", s -> tedJobs::syncData);
    tedDriver.start();
    return tedDriver;
  }
```

```java
// implements TedProcessor object
@Component
public class TedJobs {
    ...
    public TedResult syncData (TedTask task) {
        if (isEmpty(task.getData()))
            return TedResult.error("task.data is empty");
        ...
        return TedResult.done();
    }
}
```

```java
// create TedTask
tedDriver.createTask("DATA_SYN", "{\"customerId\" : \"1234\"}");
```

Mode samples can be found in (ted)/ted-samples.

## Structure

#### tedtask table

All tasks are stored into tedtask table, and ted-driver periodically check it for new tasks.
_tedtask_ table structure:

- `taskid  ` - Id - primary key
- `system  ` - System id. E.g. myapp, or myapp.me for dev
- `name    ` - Task name
- `status  ` - Status
- `channel ` - Channel
- `nextts  ` - Postponed execute (next retry) timestamp
- `batchid ` - Reference to batch taskId
- `key1    ` - Search key 1 (task specific)
- `key2    ` - Search key 2 (task specific)
- `data    ` - Task data (parameters)
- `msg     ` - Status message
- `createts` - Record create timestamp
- `startts ` - Execution start timestamp
- `finishts` - Execution finish timestamp
- `retries ` - Retry count
- `bno     ` - Internal TED field (batch number)

Task parameters can be serialized into string (e.g. to json), and stored into _data_ column.

#### Statuses
- `NEW` – new
- `WORK` – taken for process
- `RETRY` – retryable error while executing, will retry later
- `DONE` – finished successfully
- `ERROR` – error occurred, will not retry
- `SLEEP` - task is created, but will not be taken to processing yet (used for events queue)
