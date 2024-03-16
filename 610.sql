CREATE OR REPLACE PROCEDURE `prj-dfdm-121-gcmdm-p-121.bq_myprj_fdp_transform_sp.cs019610_fleet`()
begin
create or replace table bq_myprj_stg.cs019610_fleet_src_02 as (
   select * except (trdm) 
from (select *,
             count(*) over (partition by gcdd06_fleet_c, gcdd06_fltmgloc_c 
             order by gcdd06_fleet_c, gcdd06_fltmgloc_c asc) as trdm
      from (      
select distinct
gcdd06_fleet_k
,gcdd06_fleet_c
,gcdd06_fltmgloc_c
,gcdd06_flt_acct_n
,gcdd06_delin_day_t
,gcdd06_delin_trdm_y
,gcdd06_repairing_f
,gcdd06_flt_upd_y
,gcdd06_flt_mgmt_n
,gcdd06_flt_postcode1_c 
,gcdd06_flt_postcode2_c  
,gcdd06_upd_ods_s
,gcdd06_update_y
,gcdd06_update_id_c, 2 as flag from (
select  
        0 as gcdd06_fleet_k
	     ,ext_jm04_jm07_fleet_c     as gcdd06_fleet_c
       ,ext_jm04_jm04_fltmgloc_c  as gcdd06_fltmgloc_c
       ,ext_jm07_jm07_flt_acct_n  as gcdd06_flt_acct_n
       ,ext_jm07_jm07_delin_day_t as gcdd06_delin_day_t
       ,ext_jm07_jm07_delin_trdm_y as gcdd06_delin_trdm_y
       ,ext_jm04_jm04_repairing_f as gcdd06_repairing_f
       ,ext_jm04_jm04_flt_upd_y   as gcdd06_flt_upd_y
       ,ext_jm04_jm04_flt_mgmt_n  as gcdd06_flt_mgmt_n
       ,ext_jm04_jm04_postcode1_c   as gcdd06_flt_postcode1_c 
       ,ext_jm04_jm04_postcode2_c   as gcdd06_flt_postcode2_c  
	  , (case when  ext_jm07_jm07_update_s > ext_jm04_jm04_update_s         
        then ext_jm07_jm07_update_s       
     else                                                     
         ext_jm04_jm04_update_s end) as gcdd06_upd_ods_s
	  ,current_date() as gcdd06_update_y  
	  ,'cs019610' as gcdd06_update_id_c  
from  bq_myprj_stg.fleet_19600_src_01
where ext_jm04_jm07_fleet_c  is not null and trim(ext_jm04_jm07_fleet_c)  !='' and ext_jm04_jm04_fltmgloc_c is not null and trim(ext_jm04_jm04_fltmgloc_c) !=''

order by ext_jm04_jm07_fleet_c, ext_jm04_jm04_fltmgloc_c ASC ,ext_jm07_jm07_update_s desc)
) x  )

WHERE trdm = 1) ;

create or replace table bq_myprj_stg.cs019610_fleet_src_all as
select distinct *  from (
select * except (trdm) 
from (select *,
             count(*) over (partition by gcdd06_fleet_c, gcdd06_fltmgloc_c ,gcdd06_upd_ods_s
             order by gcdd06_fleet_c, gcdd06_fltmgloc_c asc ,gcdd06_upd_ods_s desc ) as trdm
      from ( 
 
select * from bq_myprj_stg.cs019610_fleet_src_02_yst
union all
select * from bq_myprj_stg.cs019610_fleet_src_02
) 
order by gcdd06_fleet_c, gcdd06_fltmgloc_c ASC ,gcdd06_upd_ods_s desc ) where trdm= 1  )

where flag = 2;

create or replace table bq_myprj_stg.cs019610_fleet_src_02_yst as 
select * from bq_myprj_stg.cs019610_fleet_src_all ;

update bq_myprj_stg.cs019610_fleet_src_02_yst  
set flag=1 
where flag=2 ;
-------backup taken for yesterday data
export data
  options (
    uri = 'gs://bkt-tfstate-dfdm-121-myprj-p/cs019610_fleet_yst_backup_' || current_timestamp() || '_*.csv',
    format = 'csv',
    overwrite = true,
    header = true,
    field_delimiter = ';')
as (
  select * from bq_myprj_stg.cs019610_fleet_src_02_yst
);
end;