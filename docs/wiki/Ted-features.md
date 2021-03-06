# Ted

Tasks registered into _tedtask_ table and then TED process them. 
As TED is as part of app (jar), it does not require separate process.
Every app instance check for new tasks, reserves portion of them and retrieves to process.

## Main features

### TedDriver configuration
Tasks should be registered in initialization phase, their configuration provided as properties (can be read from *.properties file, see _ted-sample.properties_ under test/resources dir in project)

There can be use such properties:

##### Driver internals
* **ted.systemId** - few systems can share same db table for their tasks. To separate those tasks, a systemId must be provided. The systemId must be unique among other systems. Each system will take only its own tasks. Can be useful and for testing/development purpose also - having own system id will prevent from stealing tasks by other developers.
* ted.driver.**intervalDriverMs** - how frequently driver will check for new tasks in db, default 700(ms)
* ted.driver.**intervalMaintenanceMs** - how frequently driver will do maintenance tasks, default 50000(ms)
* ted.maintenance.**oldTaskArchiveDays** - how long to keep finished tasks in days (later they will be deleted), default 35

##### Task default settings (will be used if not defined for task individually)
* ted.taskDefault.**retryPauses** - intervals for retries, default "12s,36s,90s,300s,16m,50m,2h,5h,7h*5;dispersion=10"
* ted.taskDefault.**timeoutMinutes** - task in status WORK longer than this time will be considered as dead and it's status will be changed to RETRY, default 30 minutes;
* ted.taskDefault.**batchTimeoutMinutes** - timeout for batch tasks (how long to wait for finish all subtasks), default 180


### Tasks

TED regularly check for new tasks in db. If there are suitable tasks, then it will reserve portion of them (sets status WORK) and retrieves to processing.

Ted task is separate task with own status, own data.

As task is registered in db table, it has some advantages:
* task will remain after shutdown of app,
* it could be executed by any of app instance (balanced),
* there can be created thousands of new tasks w/o afraid of memory consumption,
* db contains task execution history, errors,
* it is possible to restart task, 
* tasks can be registered by external system, e.g. for data replication.
* and more...

There may be few drawback as well:
* there can be some latency (up to ~0.7s),
* required db (PostgreSQL or Oracle)

Task must be registered in Java code (method `registerTaskConfig`). But it's configuration can be set up in this file (these parameters will overwrite ones in Java).
Task configuration will be recognized by prefix "ted.task.<TASK>".
Task parameters:
* ted.task.MYTASK.**channel** - to which channel assign task
* ted.task.MYTASK.**retryPauses** - retry pauses, if task require different retry times than default
* ted.task.MYTASK.**timeoutMinutes** - if task is long-term (executes longer than 30 min), timeout should be increased

Example of task configuration
> `ted.task.CHKSTA.channel = IDLE`  
> `ted.task.CHKSTA.retryPauses = 20s,10s*5,1m*20,5m*100;dispersion=10`

On task registration, `TedProcessorFactory` must be provided. This factory returns `TedProcessor` for each task.
`TedProcessor` process task and return one of 3 result status:
* TedResult.done() - task was successfully executed;
* TedResult.retry() - task should be retried;
* TedResult.error() - task finished with error, will not retried.


### Channels
To avoid situation, when a lot of tasks of one type blocks other type tasks, 
different channels can be assigned to tasks.
Each channel has separate thread pool with own configuration,
thus tasks in one channel will not block tasks from another channel.
Channels can be configured descriptive - using properties, not program code.

Channels configuration will be recognized by prefix "ted.channel.<CHANNEL>".
Channel parameters:
* ted.channel.MAIN.**workerCount** - thread count for this channel
* ted.channel.MAIN.**taskBuffer** - buffer of task. Pack of tasks will be retrieved from db and will be held in this buffer to wait for execution
* ted.channel.MAIN.**primeOnly** - if 'yes', then will be active only in prime (one) instance

Channel name is limited to 5 symbols of letters or numbers

The MAIN channel is default for task and will be created anyway. Other channels are optional and can be configured for various purposes.


### Retrying

When processing task happens temporal errors, like lock'ing or external resource not acceptable, then task can be set to status RETRY.
Retrying can be repeated several time until task will be executed or marked with status ERROR.
It is possible to configure task's retry policy in ted.properties.

E.g. `ted.task.MYTASK.retryPauses = 20s,30s*5,2m*100;dispersion=10` first retry will be after 20s, then 5 times every 30s, then 100 times every 2 minutes. Periods will be calculated with 10% dispersion. If tasks will not be successfully executed after all retries, then finally it will be marked as ERROR.

`TedRetryScheduler` allows to programmatically configure non-standard logic, e.g. exact time in day, skip weekends an etc.

### Batches

TED can be used in various cases, one of then can be batch processing.
Big job can be split into many tasks, which will be executed separately, in all instances.
After finishing of all tasks, a 'batch' task will be called to finish the job.

To create batch task an api method `createBatch` is provided.
It will create all tasks, provided by parameter, and special _batch task_. 
The _batch task_ will be processed only after all child task finished.


[[Ted features additional]] some other features (for PostgreSql db)  
