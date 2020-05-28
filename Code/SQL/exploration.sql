------------------------------------------------
---------- SNOWFLAKE DATA EXPLORATION ----------
-------- AUTHORED BY JUAN PABLO URRUTIA --------
------------------------------------------------

select * from <schema>.<table>
where records['os']::varchar = 'Linux'
limit 10;

select distinct min(ts) as earliest_date
 ,max(ts) as latest_date
from cpu_health_linux_view_test;


select * from <schema>.cpu_health_linux_view;


-- EXPLORATION
create or replace view <schema>.<table>_view as
select 
distinct org_name
from <schema>.<table>
where ts > '2020-04-01 00:00:00.000' AND ts < '2020-04-17 00:00:00.000' limit 10000;
--AND org_name = 'Southern Glazers'
--limit 50000000;


-- EXPLORATION
select split(split(counters['Path'],'\\')[3],'(')[0]::varchar as test 
from <schema>.<table>_view limit 10;

-- EXPLORATION
select split(split('\\\\stw-poc-04\\processor(_total)\\% processor time','\\')[3],'(')[0]::varchar;  

-- EXPLORATION
create or replace view <schema>.<table>_view as
select 
 b.org_name
,a.CC_KEY	
,a.OS
,a.PROCESS_GROUP
,a.HOSTNAME
,a.METRIC_CATEGORY	
,a.METRIC
,a.PROCESS
,a.TS	
,a.VALUE
,a.OSVER	
,a.PROC_CORES
from <database>.<schema>.<table> a
join <database>.matillion.projects b
on a.cc_key = b.cc_key
where a.ts::date between '2020-04-01' AND '2020-04-17' 
AND b.org_name = 'JLL';

-- EXPLORATION
select count(1) from <database>.<schema>.<table> where ts::date between '2020-04-01 00:00:00.000' AND '2020-04-17 23:00:00.000';





    
-- EXPLORATION
select distinct min(_load_time) as earliest_date
 ,max(_load_time) as latest_date
from "<database>"."<schema>"."<table>";

-- EXPLORATION
select count(1) from "<schema>"."<table>";




-- EXPLORATION
select *  
from <schema>.<table>(information_schema.query_history(dateadd('days',-1,current_timestamp()),current_timestamp()))
order by start_time;

-- EXPLORATION
select *
from "<database>"."<schema>"."<table>"(information_schema.query_history(dateadd('hours',-1,current_timestamp()),current_timestamp()))
order by start_time;

-- EXPLORATION
create transient table t_cpu_health as select * from "<database>"."<schema>"."<table>";

-- EXPLORATION
select count(1) from <schema>."<table>_VIEW";

-- EXPLORATION
select count(1)
  from <schema>.<table>, 
  lateral flatten(input => records['counters'])
  where split(_file_name,'/')[1] = 'Bain';

-- EXPLORATION
--select distinct days(ts) from <schema>.<table>;
select * from <schema>.<table>_view where ts >= '2020-04-01 00:00:00.000' AND ts <= '2020-04-17 00:00:00.000' limit 10;

-- EXPLORATION
create or replace view <table>_view as
select 
*
from <schema>.<table>
where ts >= '2020-04-01 00:00:00.000' AND ts <= '2020-04-17 00:00:00.000' 
AND org_name = 'Southern Glazers';

-- EXPLORATION
select
distinct org_name
from <schema>.<table>
where  ts >= '2020-04-01 00:00:00.000' OR ts <= '2020-04-17 00:00:00.000';

-- EXPLORATION
select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('day',-6,current_timestamp()),
    result_limit=>5,
    task_name=>'<schema>_<table>_STREAM_PROCEDURE_TASK'));

-- EXPLORATION
select * 
from <schema>.<table>(information_schema.pipe_usage_history(
  date_range_start => to_timestamp_tz('2020-02-01 00:00:00.000'),
  date_range_end => to_timestamp_tz('2020-04-30 00:00:00.000')).);

-- EXPLORATION
select *
  from table(information_schema.pipe_usage_history(
    date_range_start=>to_timestamp_tz('2017-10-24 12:00:00.000 -0700'),
    date_range_end=>to_timestamp_tz('2017-10-24 12:30:00.000 -0700')));


-- EXPLORATION
SELECT *
FROM table(
  information_schema.task_history(
    task_name=>'<schema>_<table>_STREAM_PROCEDURE_TASK'
    ,scheduled_time_range_start=>dateadd('day',-6,current_timestamp())
  )
);

SELECT *
from table(information_schema.task_history(
  scheduled_time_range_start=>to_timestamp_ltz('2020-04-16 12:00:00.000 -0700'),
  scheduled_time_range_end=>to_timestamp_ltz('2018-04-18 12:30:00.000 -0700')));

/*
select 
records
from <schema>.<table>
where ORG_NAME = 'Southern Glazers'
limit 10;
*/

//select * from <schema>.<table> where OS = 'Linux'  limit 1;

--desc procedure NEW_RECORD_STORED_PROCEDURE();

-- EXPLORATION
/*
create or replace view <schema>.cpu_performance as
select 
records
,records['ver']::varchar as ver// first way to parse the json
,parse_json(records):ver::varchar as ver_alt// another way to parse the json
,records['os']::varchar as os
,parse_json(records):os::varchar as os_alt
,records['cc']::varchar as cc
,records['diskstat1']['1']['time_spend_reading_ms']::varchar as diskstat_time_reading_ms
,records['ts']::timestamp as ts // cast a string timestamp to an actual timestamp in Snowflake
,records['host']::varchar as host
,records['diskstat0']['1']['device_name']::varchar as device_name
from <schema>.<table>;

select * from <schema>.cpu_performance;
*/

    




