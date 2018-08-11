# TED - Task Execution Driver

## About

TED is library for Java to handle task execution.

Tasks are created in Oracle/Postgres db (table _tedtask_). 
TED will check for new tasks in that table, retrieves them, call task processor and after finish will set status (DONE, ERROR or RETRY).

#### Main things kept in mind while creating TED
- simplicity of use;
- good performance;
- no jar dependencies;
- easy support - it is possible to view task history in db, restart task;
- can be used for short (~50ms) and long (~5h) tasks;

#### Features
- is a part of war (or other app), there are no needs for separate process;
- tasks are in db, thus tasks remains after restart of tomcat;
- it is possible to easy view tasks history;
- there are auto cleaning after 35 days;
- task will be executed only by 1 application instance (any);
- works with PostgreSQL (9.5+) or Oracle DB;
- task configuration is in separate ted.properties file;
- it is possible to retry task by it's own retry policy;
- ensures only one prime (main) instance of few app nodes at same time; after shutdown of prime node another node will become prime;
 
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


Where TED isn't best solution
- (very) intensive data processing, streaming, where latency milliseconds matters, like real-time client request handling, heavy db replication and etc;
- as applications integration solution (e.g. one app can create task for other app);
- nuclear reactor management;

### Comparision 

Difference from spring-batch
- spring-batch is designed for big batch processing, while TED approach is to divide big batch into independent small(er) tasks, which will have their own live cycle (e.g. instead of creating 50k reports in one batch, in TED scenario you will need to create 50k separate tasks). While it is possible to execute and long-running tasks in TED, but task's internal logic is not interesting for TED.

Difference from quartz
- TED is not designed as scheduler, but it is possible to use it as simple scheduler (e.g. tasks with settings retryPauses=2h*999999 may work like scheduler) 
 
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

```java
// create TedTask
tedDriver.createTask("DATA_SYN", "{\"customerId\" : \"1234\"}");
```

Mode samples can be found in (ted)/ted-samples

## Structure

#### tedtask table

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

#### Statuses
- `NEW` – new
- `WORK` – taken for process
- `RETRY` – retryable error while executing, will retry later
- `DONE` – finished successfully
- `ERROR` – error occurred, will not retry
- `SLEEP` - task is created, but will not be taken to processing yet (used for events queue)

#### Retry pauses
It is possible to configure task's retry policy in ted.properties.

E.g. `ted.task.MYTASK.retryPauses=20s,30s*5,2m*100;dispersion=10` first retry will be after 20s, then 5 times every 30s, then 100 times every 2 minutes. Periods will be calculated with 10% dispersion.

`TedRetryScheduler` allows to programmatically configure non-standard logic.    

#### Channels
Each channel is separate thread pool with own configuration.
They can be used when different priorities are required for different tasks. 

#### Prime instance
It is possible to use TED for managed prime (master) instance between few instances/nodes.

#### Event queue
With _event queues_ it is possible to execute tasks in strictly the same sequence, as they were created.

For more details see 
[Ted features](docs/wiki/Ted-features.md) in wiki
