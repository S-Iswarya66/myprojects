begin

declare start_cdc_date1 timestamp;
declare last_cdc_dt_update timestamp;
declare ext_cycle_run1 int64;
declare cutoff_ts timestamp;
declare ext_cycle_run2 int64;

set start_cdc_date1 =  timestamp_sub((select max(last_cdc_dt) from bq_myprj_opr.job_details where load_name = 'cs019600_fleet'), interval 12 hour);
set cutoff_ts = timestamp_sub((select max(start_ts)from bq_myprj_opr.batch_load_run),interval 15 minute);
execute immediate "select max(ext_cycle_run) from `bq_myprj_opr.job_run_details` where load_name = 'cs019600_fleet'" into ext_cycle_run1;
set ext_cycle_run2 = ifnull(ext_cycle_run1,0) +1 ;

select ext_cycle_run2 ;
insert into `bq_myprj_opr.job_run_details`(ext_cycle_run,load_name,project_name,dataset,job_name,target_table,start_cdc_ts,end_cdc_ts,status) values (ext_cycle_run2,'cs019600_fleet','myprj_project_121','`bq_myprj_fdp_extract_sp.cs019600_fleet`','cs019600','sgcdd06_fsa_fleet_tbl',start_cdc_date1,cutoff_ts,'inprogress');


create or replace table bq_myprj_stg.fleet_19600_src_01 as
select  jm04.gcjm07_fleet_c   as ext_jm04_jm07_fleet_c                      
      , jm04.gcjm04_fltmgloc_c  as ext_jm04_jm04_fltmgloc_c                    
      , jm04.gcjm04_repairing_f  as ext_jm04_jm04_repairing_f                   
      , jm04.gcjm04_flt_upd_y     as ext_jm04_jm04_flt_upd_y                   
      , jm04.gcjm04_flt_mgmt_n    as ext_jm04_jm04_flt_mgmt_n                   
      , jm04.gcjm04_postcode1_c   as ext_jm04_jm04_postcode1_c                  
      , jm04.gcjm04_postcode2_c   as ext_jm04_jm04_postcode2_c                   
      , jm04.gcjm04_update_s      as ext_jm04_jm04_update_s                  
      , jm07.gcjm07_flt_acct_n  as ext_jm07_jm07_flt_acct_n                    
      , jm07.gcjm07_delin_day_t as ext_jm07_jm07_delin_day_t                    
      , jm07.gcjm07_delin_cnt_y as ext_jm07_jm07_delin_cnt_y                    
      , jm07.gcjm07_update_s   as ext_jm07_jm07_update_s 
	  , current_date() as job_run_date
	  , current_timestamp() as job_run_time
	  , 'cs019600' as job_name
	  , ext_cycle_run2 as pg01_extrcycl_n
from    `prj-dfdl-79-myprj-p-0079.bq_79_myprj_lnd_lc_vw.sgcjm04_fleet_mgmt_vw` jm04 
,`prj-dfdl-79-myprj-p-0079.bq_79_myprj_lnd_lc_vw.sgcjm07_fleet_vw`     jm07                     
WHERE jm04.gcjm07_fleet_c =  jm07.gcjm07_fleet_c      
AND  
         ((jm04.df_bq_update_ts   >= start_cdc_date1
and     jm04.df_bq_update_ts   <  cutoff_ts ) 
or     (jm07.df_bq_update_ts   >= start_cdc_date1 
and     jm07.df_bq_update_ts   < cutoff_ts ))
order by jm04.gcjm07_fleet_c,                          
         jm04.gcjm04_fltmgloc_c;
                          

insert into `bq_myprj_opr.job_run_details`(ext_cycle_run,load_name,project_name,dataset,job_name,target_table,start_cdc_ts,end_cdc_ts,status) values (ext_cycle_run2,'cs019600_fleet','myprj_project_121',
'`bq_myprj_fdp_extract_sp.cs019600_fleet`','cs019600','sgcdd06_fsa_fleet_tbl',start_cdc_date1,cutoff_ts,'success');

set last_cdc_dt_update= (select max(end_cdc_ts) from bq_myprj_opr.job_run_details where  target_table='sgcdd06_fsa_fleet_tbl' and load_name = 'cs019600_fleet' and  status = 'success');

update bq_myprj_opr.job_details
set last_cdc_dt= last_cdc_dt_update
where load_name = 'cs019600_fleet' ;   
             
-- exception when error then

-- insert into `bq_myprj_opr.job_run_details`(ext_cycle_run,load_name,project_name,dataset,job_name,target_table,start_cdc_ts,end_cdc_ts,status,error_check) values (ext_cycle_run2,'cs019600_fleet','myprj_project_121',
-- '`bq_myprj_fdp_extract_sp.cs019600_fleet`','cs019600','sgcdd06_fsa_fleet_tbl',start_cdc_date1,cutoff_ts,'failed',@@error.message);
end;