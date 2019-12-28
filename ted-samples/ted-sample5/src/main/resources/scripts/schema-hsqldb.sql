
create sequence seq_tedtask_id minvalue 1000 maxvalue 999999999 start with 1000 increment by 1;
create sequence seq_tedtask_bno minvalue 1000 maxvalue 999999999 start with 1000 increment by 1;

create table tedtask(
    taskid      bigint not null,
    system      varchar(8) not null, -- system id. e.g. myapp
    name        varchar(15) not null,
    status      varchar(5) not null, -- NEW, WORK, RETRY, ERROR, DONE, SLEEP
    channel     varchar(5), -- channel: MAIN
    nextts      timestamp(3) default now(),
    batchid     bigint null,
    retries     integer default 0 not null,
    key1        varchar(30),
    key2        varchar(30),
    createts    timestamp(3) default now() not null,
    startts     timestamp(3),
    finishts    timestamp(3),
    msg         varchar(1000),
    data        varchar(1000),
    bno         bigint
    );

create unique index ix_tedtask_pk on tedtask (taskid);

