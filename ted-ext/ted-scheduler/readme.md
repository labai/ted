# Ted scheduler

## About

Helper for creating scheduler tasks.
 
Use ted-driver for task executing.
Provides simplified API for scheduler tasks,
some additional functionality, e.g. cron expressions.


##### In Spring

```java
@Service
public class SchedulerTasks {
    
	@TedSchedulerProcessor(name = "SCH_1", cron = "1 * * * * *")
	public String schedulerTask1() {
		logger.info("Start schedulerTask1");
		return "ok";
	}

}
```

##### Manual

```java
TedScheduler scheduler = new TedScheduler(tedDriver);
scheduler.builder().name("MAINT")
        .scheduleCron("0 0/10 * 1/1 * ?")
        .runnable(() -> {
            logger.info("do something");
        }).register();
```

When registering task, TedScheduler also checks, do exists such task in db, 
and if not exists, then creates it.

Technically TedScheduler creates Ted tasks, but they will have some restrictions:
- they have never ending retry policy - they always be in status RETRY (cron expression can be used to calculate next execution time); 
- thus it is not allowed to return error while executing tasks. TedScheduler will catch error and return RETRY;
- if somehow task status will be set to ERROR in db, TedScheduler will restore it to RETRY;
- there can be only one active scheduler task of one type;
