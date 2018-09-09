drop table tedtask;

create table tedtask(
    taskid      bigint not null auto_increment,
    `system`    varchar(8) not null, -- system id. e.g. myapp
    name        varchar(15) not null,
    status      varchar(5) not null, -- NEW, WORK, RETRY, ERROR, DONE, SLEEP
    channel     varchar(5), -- channel: MAIN
    nextts      timestamp(3) default now(3),
    batchid     bigint null,
    retries     integer default 0 not null,
    key1        varchar(30),
    key2        varchar(30),
    createts    timestamp(3) not null default now(3),
    startts     timestamp(3),
    finishts    timestamp(3),
    msg         varchar(300),
    data        mediumtext,
    bno         bigint,
    primary key (taskid)
);

alter table tedtask auto_increment = 1000;

-- Indexes for tedtask.
-- Warning: It is expected that tedtask table is for single app (system). In case of few apps (system) use one tedtask, it may be better to use other indexes
-- create unique index ix_tedtask_pk on tedtask (taskid);
create index ix_tedtask_batchid on tedtask (batchid, status);
create index ix_tedtask_name on tedtask (name, status);
create index ix_tedtask_quickchk on tedtask (nextts);
create index ix_tedtask_nextts on tedtask (channel, `system`, nextts);
create index ix_tedtask_key1 on tedtask (key1);
create index ix_tedtask_key2 on tedtask (key2);
create index ix_tedtask_createts on tedtask (createts);
create index ix_tedtask_bno on tedtask (bno);
create index ix_tedtask_status on tedtask (status);
