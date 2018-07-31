drop table tedtask;
drop sequence seq_tedtask_id;
drop sequence seq_tedtask_bno;

create sequence seq_tedtask_id minvalue 1000 maxvalue 999999999999 start with 1000 increment by 1;
create sequence seq_tedtask_bno minvalue 1000 maxvalue 999999999999 start with 1000 increment by 1;

create table tedtask(
    taskid      bigint not null,
    system      varchar(8) not null, -- system id. e.g. myapp
    name        varchar(15) not null,
    status      varchar(5) not null, -- NEW, WORK, RETRY, ERROR, DONE
    channel     varchar(5), -- channel: MAIN
    nextts      timestamp(3) default now(),
    batchid     bigint null,
    retries     integer default 0 not null,
    key1        varchar(30),
    key2        varchar(30),
    createts    timestamp(3) default now() not null,
    startts     timestamp(3),
    finishts    timestamp(3),
    msg         varchar,
    data        varchar,
    bno         bigint
    );


-- Indexes for tedtask.
-- Warning: It is expected that tedtask table is for single app (system). In case of few apps use one tedtask, it may be better to use other indexes
create unique index ix_tedtask_pk on tedtask (taskid);
create index ix_tedtask_batchid on tedtask (batchid, status);
create index ix_tedtask_name on tedtask (name, status);
--create index ix_tedtask_quickchk on tedtask (nextts);
create index ix_tedtask_nextts on tedtask (channel, system, nextts);
create index ix_tedtask_key1 on tedtask (key1);
-- unique key1 - to ensure maximum one active task for some object
-- create unique index ix_tedtask_key1_uniq on tedtask (key1, name)
--    where status in ('NEW', 'WORK', 'RETRY'); -- ... not key1 is null and key1 > ''
create index ix_tedtask_key2 on tedtask (key2);
create index ix_tedtask_createts on tedtask (createts);
create index ix_tedtask_bno on tedtask (bno);
create index ix_tedtask_status on tedtask (status); -- maintenance task


-- Comments for tedtask
comment on table tedtask is 'TED (Task Execution Driver) tasks';

comment on column tedtask.taskid     is 'Id - primary key';
comment on column tedtask.batchid    is 'Reference to batch taskId';
comment on column tedtask.system     is 'System id. E.g. "myapp"';
comment on column tedtask.name       is 'Task name';
comment on column tedtask.status     is 'Status';
comment on column tedtask.channel    is 'Channel';
comment on column tedtask.msg        is 'Status message';
comment on column tedtask.createts   is 'Record create timestamp';
comment on column tedtask.startts    is 'Execution start timestamp';
comment on column tedtask.finishts   is 'Execution finish timestamp';
comment on column tedtask.retries    is 'Retry count';
comment on column tedtask.nextts     is 'Postponed execute (next retry) timestamp';
comment on column tedtask.key1       is 'Search key 1 (task specific)';
comment on column tedtask.key2       is 'Search key 2 (task specific)';
comment on column tedtask.data       is 'Task data (parameters)';
comment on column tedtask.bno        is 'Internal TED field (batch number)';
