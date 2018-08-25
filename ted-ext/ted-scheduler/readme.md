# Ted scheduler

## About

Helper for creating scheduler tasks.
 
Use ted-driver for task executing.
Provides simplified API for scheduler tasks,
some additional functionality, e.g. cron expressions.


```java
TedScheduler scheduler = new TedScheduler(tedDriver);
scheduler.builder().name("MAINT")
        .scheduleCron("0 0/10 * 1/1 * ?")
        .runnable(() -> {
            logger.info("do something");
        }).register();
```

When registering task, TedScheduler also will check, do exists such task in db, 
and if not exists, then creates it.


TedScheduler creates "normal" Ted tasks, but they will have some limitations:
- they have never ending retry policy - they always be in status RETRY;
- thus it is not allowed to return error while executing tasks. TedScheduler will catch error and return RETRY;
- if somehow task status will be set to ERROR in db, TedScheduler will restore it to RETRY;
- there can be only one active scheduler task of one type (NEW, RETRY, WORK or ERROR);
