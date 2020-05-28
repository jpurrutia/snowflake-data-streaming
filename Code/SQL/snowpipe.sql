-----------------------------------------------
------ CONVERTING PYTHON LAMBDA INTO SQL ------
-------- AUTHORED BY JUAN PABLO URRUTIA -------
-----------------------------------------------

-- Create <schema>
create <schema> raw;

show <schema>s;

-- Create Table
create or replace table raw.<table> (
records variant
,_sha256 varchar
,_load_time timestamp_tz
,_file_name varchar
,_file_row_number number
);

show <table>;

-- Create Stream
create or replace stream <schema>.<table>_stream on table <schema>.<table> append_only = true;

show streams;

-- Storage Integration
grant usage on integration 'STORAGE_INTEGRATION' to role 'role';
show integrations;

-- Create Stage
create or replace stage <schema>.<table>_stage
url = '<s3://PATH'
storage_integration = 'storage_integration';

show stages;

ls @<schema>.<table>_stage;

-- Create File Format
create or replace file format schema.json_file_format
type = 'JSON'
compression = auto
enable_octal = false
allow_duplicate = false
strip_outer_array = true
strip_null_values = false
ignore_utf8_errors = false;

show file formats;

-- Returns an AWS IAM policy statement that must be added to the Amazon SNS topic policy in order to grant the Amazon SQS messaging queue created 
-- by Snowflake to subscribe to the topic.
select system$get_aws_sns_iam_policy('<iam-policy>');

-- Create Pipe
create or replace pipe schema.<table>_pipe 
auto_ingest=true 
aws_sns_topic='<aws-sns-arn>'
as
copy into <schema>.<table> (records, _sha256, _load_time, _file_name, _file_row_number)
from (
  select 
  $1
  ,sha2($1)
  ,current_timestamp::timestamp_tz
  ,metadata$filename
  ,metadata$file_row_number
  from @<schema>.<table>_stage
  )
file_format = (format_name = json_file_format)
on_error = skip_file;

show pipes;

select system$pipe_status('<schema>.pipe');

select * 
from table(
  information_<schema>.copy_history(
    table_name=>'<table>'
    ,start_time=> dateadd(hours, -1, current_timestamp())
  )
);

alter pipe <schema>.<table>_pipe refresh;

select records
from <schema>.<table>;

create or replace view cpu_health_perf_view as
select
  split(_file_name,'/')[1]::varchar as org_name
  ,records['cc']::varchar as cc_key
  ,records['os']::varchar as os
  ,counters.value['InstanceName']::varchar as process_group
  ,split(counters.value['Path']::varchar,'\\')[2]::varchar as hostname
  ,split(counters.value['Path']::varchar,'\\')[3]::varchar as metric_category
  ,split(counters.value['Path']::varchar,'\\')[4]::varchar as metric
  ,trim(substr(value:Path,regexp_instr(value:Path,'#')+1,2),'\\)') as process
  ,to_timestamp(strtok(counters.value['Timestamp']::varchar,'/Date()',1)) as ts
  ,counters.value['CookedValue']::float as value
  from <schema>.<table>, 
  lateral flatten(input => records['counters']) counters;
  
show <table>;

// Stored Procedure
create or replace procedure new_record_stored_procedure()
returns string not null
language javascript
as
$$

var insert_cmd = '
insert into cpu_health_perf
select * ;
'
var sql_insert = snowflake.createStatement({sqlText: cmd}); 
var inserr_result = 
var result = sql_insert.execute();

var stream_select_cmd = '
insert into <database>.<schema>.table
select
split(_file_name,'/')[1]::varchar as org_name
,records['cc']::varchar as cc_key
,records['os']::varchar as os
,counters.value['InstanceName']::varchar as process_group
,split(counters.value['Path']::varchar,'\\')[2]::varchar as hostname
,split(counters.value['Path']::varchar,'\\')[3]::varchar as metric_category
,split(counters.value['Path']::varchar,'\\')[4]::varchar as metric
,trim(substr(value:Path,regexp_instr(value:Path,'#')+1,2),'\\)') as process
,to_timestamp(strtok(counters.value['Timestamp']::varchar,'/Date()',1)) as ts
,counters.value['CookedValue']::float as value
,hour(ts) as hour
from <database>.<schema>.<table>_stream, 
lateral flatten(input => records['counters']) counters
where metadata$action = 'INSERT';
'
var sql_select_stream = snowflake.createStatment({sqlText: stream_select_cmd});
var select_stream_result = sql_select_stream.execute();
return 'Success';
$$;
show procedures;
desc procedures;

drop task <schema>_<table>_task;


// Create Stream
create or replace stream <schema>_<table>_stream on table <schema>.<table> append_only = True;
show streams;

// Create Task
create or replace task <schema>_<table>_task
    warehouse = <database>_wh
    schedule = '15 MINUTE'
when
    system$stream_has_data('<schema>_<table>_stream')
as
    call new_record_stored_procedure();
    
alter task <schema>_<table>_task resume;
 
show tasks;