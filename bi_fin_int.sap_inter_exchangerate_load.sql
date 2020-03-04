Create or replace procedure bi_fin_int.sap_inter_exchangerate_load() language plpgsql
as
'
Declare
lv_count int :=0;
lv_rec_count int;
lv_error varchar(500);
lv_start_time timestamp;
lv_end_time   timestamp;
v_table_name varchar(255);


Begin

lv_start_time := sysdate; 


Select nvl(max(seq_no),0) +1 into lv_count from bi_exec_log;

insert into bi_fin_int.sap_inter_exchangerate
(
absoluteexchangerate,
directquotedeffectiveexchra,
effectiveexchangerate,
exchangerate,
exchangerateeffectivedate,
exchangeratetype,
exchrateisindirectquotation,
indirectquotedeffctvexchrat,
invertedexchangerateisallow,
numberofsourcecurrencyunits,
numberoftargetcurrencyunits,
sourcecurrency,
sourcecurrencydecimals,
specialconversionisused,
targetcurrency,
targetcurrencydecimals,
partition_date,
system_created_date
)
Select 
absoluteexchangerate,
directquotedeffectiveexchra,
effectiveexchangerate,
exchangerate,
exchangerateeffectivedate,
exchangeratetype,
exchrateisindirectquotation,
indirectquotedeffctvexchrat,
invertedexchangerateisallow,
numberofsourcecurrencyunits,
numberoftargetcurrencyunits,
sourcecurrency,
sourcecurrencydecimals,
specialconversionisused,
targetcurrency,
targetcurrencydecimals,
partition_date,
getdate()
from  bi_fin_stg.sap_stage_exchangerate;

--from  bi_fin_stg.sap_stage_exchangerate where partition_date = (Select max(a.partition_date) from bi_fin_stg.sap_stage_exchangerate a);


GET DIAGNOSTICS lv_rec_count := ROW_COUNT;   

lv_end_time := sysdate;

Insert into bi_exec_log values (lv_count,''FIN'',''sap_inter_exchangerate_load'',''sap_inter_exchangerate'',lv_error,''S'',lv_rec_count,lv_start_time,lv_end_time,sysdate,user);

-------------------------------------------create snap shot-------------------------------------------

 v_table_name := ''bi_fin_int.sap_inter_exchangerate''||''_''||to_char(sysdate,''ddmmyyyyhhmiss'');
execute ''CREATE TABLE '' ||v_table_name||'' as Select * from ''|| ''bi_fin_int.sap_inter_exchangerate'';




EXCEPTION WHEN OTHERS THEN 
      RAISE exception ''Exception message SQLERRM %'', SQLERRM ;
      RAISE exception ''Exception message SQLSTATE %'', SQLSTATE;
  
End;
;'


