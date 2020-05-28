-----------------------------------------------
------ CONVERTING PYTHON LAMBDA INTO SQL ------
-------- AUTHORED BY JUAN PABLO URRUTIA -------
-----------------------------------------------


----------------------------------
----- WINDOWS LAMBDA PORTING -----
----------------------------------
create or replace view cpu_health_perf as
select
  split(_file_name,'/')[1]::varchar as org_name
  ,records['cc']::varchar as cc_key
  ,records['os']::varchar as os
  ,counters.value['InstanceName']::varchar as process_group
  ,split(counters.value['Path']::varchar,'\\')[2]::varchar as hostname
  ,split(split(counters.value['Path']::varchar,'\\')[3],'(')[0]::varchar as metric_category
  ,split(counters.value['Path']::varchar,'\\')[4]::varchar as metric
  ,trim(substr(value:Path,regexp_instr(value:Path,'#')+1,2),'\\)') as process
  ,to_timestamp(strtok(counters.value['Timestamp']::varchar,'/Date()',1)) as ts
  ,counters.value['CookedValue']::float as value
  ,records['osver']::varchar as osver
  ,records['proc_cores']::number as proc_cores
  ,records['proc_lp']::number as proc_lp
  ,records['ramgb']::number as ramgb
  ,records['updays']::number as updays
  from raw.events, 
  lateral flatten(input => records['counters']) counters;

--------------------------------
----- LINUX LAMBDA PORTING -----
--------------------------------

create or replace view cpu_health_linux_view_test
as
select
    split(_file_name,'/')[1]::varchar as org_name
    ,records['cc']::varchar AS cc_Key
    ,records['host']::varchar AS hostname
    ,records['ts']::timestamp AS ts
    ,pm.value['comm']::varchar as process_group
    ,pm.value['pid']::varchar as process
    ,((ds1.value['weighted_time_spent_doing_ios_ms'] - ds0.value['weighted_time_spent_doing_ios_ms']) / 1000)::varchar as active_time_pct
    ,records['freest']['mem_avail']::varchar as available_kbytes_mem
    ,df.value['available']::varchar as available_kbytes_disk
    ,records['freest']['mem_buff']::varchar as buffer_kbytes
    ,records['freest']['mem_free']::varchar as free_kbytes_mem      --------------> change here <--------------
    ,records['freest']['swap_free']::varchar as free_kbytes_swap      --------------> change here <--------------
    ,records['topst']['cpu_idl']::varchar as idle_pct
    ,((ns1.value['bytes_in'] - ns0.value['bytes_in']) / 1000)::varchar as inbound_kps
    ,ds1.value['ios_currently_in_progress']::varchar as ios_in_progress
    ,records['topst']['ld_avg1']::varchar as load_avg_1min
    ,records['topst']['num_procs']::varchar as num_procs
    ,((ns1.value['bytes_out'] - ns0.value['bytes_out']) / 1000)::varchar as outbound_kbs
    ,substr(df.value['pct'],1,len(df.value['pct'])-1)::varchar as pct_utilization
    ,pc.value['pcpu']::varchar as process_utilization_cpu       --------------> change here <--------------
    ,pm.value['pmem']::varchar as process_utilization_mem        --------------> change here <--------------
    ,((ds1.value['reads_completed_successfully'] + ds1.value['reads_merged'] - ds0.value['reads_completed_successfully'] - ds0.value['reads_merged']) / 10)::varchar as read_iops
    ,((ds1.value['time_spend_read_ms'] - ds0.value['time_spend_reading_ms']) / nullif((ds1.value['writes_completed'] - ds0.value['writes_completed']),0))::varchar as read_latency_ms
    ,((ds1.value['sectors_read'] - ds0.value['sectors_read']) / 2 / 10 / 1000)::varchar as read_throughput
    ,df.value['size']::varchar as size_kb
    ,records['topst']['cpu_sys']::varchar as sys_pct
    ,records['freest']['mem_tot']::varchar as total_kbytes_mem          --------------> change here <--------------
    ,records['freest']['swap_tot']::varchar as total_kbytes_swap        --------------> change here <--------------
    ,records['freest']['mem_used']::varchar as used_kbytes_mem          --------------> change here <--------------
    ,records['freest']['swap_used']::varchar as used_kbytes_swap        --------------> change here <--------------
    ,df.value['used']::varchar as used_kbytes_disk                      --------------> change here <--------------
    ,records['topst']['cpu_usr']::varchar as user_pct           
    ,records['topst']['cpu_wai']::varchar as wait_pct
    ,((ds1.value['writes_completed'] + ds1.value['writes_merged'] - ds0.value['writes_completed'] - ds0.value['writes_merged']) / 10)::varchar as write_iops
    ,((ds1.value['time_spend_writing_ms'] - ds0.value['time_spend_writing_ms']) / nullif((ds1.value['reads_completed_successfully'] - ds0.value['reads_completed_successfully']),0))::varchar as write_latency_ms
    ,((ds1.value['sectors_written'] - ds0.value['sectors_written']) / 2 / 10 / 1000)::varchar as write_throughput
    from servercare.raw.events,
    lateral flatten (input => records['df']) df,
    lateral flatten (input => records['diskarr']) da,
    lateral flatten (input => records['diskstat0']) ds0,
    lateral flatten (input => records['diskstat1']) ds1,
    lateral flatten (input => records['netstat0']) ns0,
    lateral flatten (input => records['netstat1']) ns1,
    lateral flatten (input => records['pmem']) pm,
    lateral flatten (input => records['proc_cpu']) pc
    where records['os']::varchar = 'Linux'
    and ts::timestamp between '2020-04-01 00:00:00.000' AND '2020-04-17  23:59:59.000'
    limit 500000;
