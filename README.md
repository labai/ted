# TED - Task Execution Driver

## About

TED is library for Java to handle task execution.

Tasks are stored in Oracle/Postgres/MySql db (table _tedtask_). 
TED checks for new tasks in that table, retrieves them, calls task processor and after finish sets status (DONE, ERROR or RETRY).

##### Main things kept in mind while creating TED
- simplicity of use;
- good performance;
- no jar dependencies;
- easy support - it is easy to view task history in db using any sql browser, restart task;
- can be used for short (milliseconds) and long (hours) tasks;

##### Features
Main features:
- is a part of war (or other app), no needs for separate process;
- tasks are in db, thus tasks remain after restart of app, it is easy to browse history ann manage using standard sql;  
- there are _auto cleaning_ after 35 days;
- task will be executed only by one application instance;
- works with PostgreSQL (9.5+), Oracle DB or MySql (8+); from Java 1.8;
- _task configuration_ is in separate ted.properties file, may be tuned without recompile;
- it is possible to retry task by it's own _retry policy_;
- _channels_ - allows to assign separate thread pools to different types of tasks; 
- [ted-scheduler](ted-ext/ted-scheduler/readme.md) allows to use ted-driver as scheduler engine
- [ted-spring-support](ted-ext/ted-spring-support/readme.md) easily integrates into spring

See [Ted features](docs/wiki/Ted-features.md) in wiki.
 
TED can be helpful when:
- you need to have task history in db table, easy browse, restart one or few tasks;
- to be sure task will remain even if app shutdowns before task finish;
- need to balance load between few instances (e.g. tomcats);
- need retry logic;

 
## Usage example

### Using ted-spring-support 

```xml
<dependency>
   <groupId>com.github.labai</groupId>
   <artifactId>ted-spring-support</artifactId>
   <version>0.3.2</version>
</dependency>
```

```java
@EnableTedTask
@Configuration
public class TedConfig {
}
```

```java
@Service   
public class Tasks {

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

and then create tasks 

```java
@Service   
public class MyService {
    @Autowired
    private TedTaskFactory tedTaskFactory;
    
    private void createTasks() {
        tedTaskFactory.createTask("TASK1", "(task parameters...)");
    }
}
```

See also [Start-to-use](docs/wiki/Start-to-use.md) and 
[Start-to-use-in-spring](docs/wiki/Start-to-use-in-spring.md) 
in wiki. More samples can be found in [ted-samples](/labai/ted/tree/master/ted-samples).


## DB Structure

#### tedtask table

All tasks are stored into _tedtask_ table. TED driver periodically checks for new tasks.
_tedtask_ table is important part of TED. 
As structure is simple and open, a standard sql or some own tools can be used to browse, monitor and manage tasks. 
_tedtask_ structure:

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
