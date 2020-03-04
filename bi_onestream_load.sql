Create or replace procedure bi_onestream.bi_onestream_load() language plpgsql
as
'
Declare
lv_count int :=0;
lv_rec_count int;
lv_rec_count1 int;
lv_rec_count2 int;
lv_rec_count3 int;
lv_rec_count4 int;
lv_rec_count5 int;
lv_rec_count6 int;
lv_rec_count7 int;
lv_rec_count8 int;
lv_table_name varchar(255);
lv_error varchar(500);
lv_start_time timestamp;
lv_end_time   timestamp;
v_table_name varchar(255);
Begin

lv_start_time := sysdate; 


Select nvl(max(seq_no),0) +1 into lv_count from bi_exec_log;

---------------acv_rp----------

insert into bi_onestream.acv_rp
select 
a.market,
a.sales_team,
a.operating_unit,
b.product_line,
a.inv_amt_funct_cur_cd,
a.source_system,
a.acv_funct :: numeric as funct_amt,
a.acv_usd :: numeric as usd_amt , 
a.acv_plan :: numeric as plan_rate_amt,
a.suse_route_to_market,
a.fiscal_year,
a.fiscal_period,
a.revenue_type,
a.program_cd_rpt,
a.dir_indir_dtl,
a.gsp_coverage,
getdate()
from bi_rptprd_int.bi_rp_suse_int_invoice a
left join bi_rptprd_int.bi_rp_suse_int_product_level b on (a.pl_cd  = b.pl_cd);

get diagnostics lv_rec_count1:= row_count; 
 
---------------------------------acv_new-------------------------------

insert into bi_onestream.acv_new
select   
distinct
c.bill_to_country,
f.sales_team,
a.companycode,
e.bd_item_material,
a.companycodecurrency, 
''SAP4'' as source_system,
0 :: numeric as  funct_amt,
0 :: numeric as usd_amt,
round (a.amountincompanycodecurrency /d.factor * d.planratetousd,2) :: numeric as plan_rate_amt,
c.route_to_market,
a.fiscalyear,
a.fiscalperiod,
b.bd_billingdocumenttype,
null as program_cd_rpt,
null as dir_indir_dtl,
null as gsp_coverage,
getdate()
from bi_fin_int.sap_inter_journalentry a
--left  join bi_fin_int.sap_inter_billingdocument b  on (a.accountingdocument = b.bd_accountingdocument)
left  join bi_fin_int.sap_inter_billingdocument b  on (a.accountingdocument = b.bd_accountingdocument and a.companycode = b.bd_companycode and a.fiscalyear = b.bd_fiscalyear) 
left  join bi_sf_int.sa_nmst_order c on (c.order_id = b.bd_yy1_sfdcorderid)
left  join bi_sf_int.sa_mst_order g on (g.order_number = c.order_id)
left  join bi_fin_int.sap_inter_fxrates d on (d.currencycode = a.companycodecurrency)
left  join bi_fin_int.sap_inter_BillingDocument_Item e on  (e.BD_Item_BillingDocument = b.bd_billingdocument)   
left  join bi_sf_int.sa_nmst_account f on (f.account_id = c.order_id);

get diagnostics lv_rec_count2:= row_count; 

---------------------------------tcv_rp-------------------------------

insert into bi_onestream.tcv_rp

select 
a.market,
a.sales_team,
a.operating_unit,
b.product_line,
a.inv_amt_funct_cur_cd,
a.source_system,
a.inv_amt_funct :: numeric  as funct_amt,
a.inv_amt_usd :: numeric as funct_amt ,
round(a.local_inv_cur_amt/c.factor * c.planratetousd,2) :: numeric as plan_rate_amt,
a.suse_route_to_market,
a.trx_date ::  timestamp,
a.fiscal_year,
a.fiscal_period,
a.revenue_type,
a.program_cd_rpt,  
a.dir_indir_dtl,
a.gsp_coverage,
getdate()
from bi_rptprd_int.bi_rp_suse_int_invoice a
left join bi_rptprd_int.bi_rp_suse_int_product_level b on (a.pl_cd  = b.pl_cd)
left join bi_fin_int.sap_inter_fxrates c on (a.local_inv_cur_cd = c.currencycode);

get diagnostics lv_rec_count3:= row_count; 

---------------------------------tcv_new-------------------------------

insert into bi_onestream.tcv_new
select   
distinct
c.bill_to_country,
f.sales_team,
a.companycode,
e.bd_item_material,
a.companycodecurrency, 
''SAP4'' as source_system,
a.amountincompanycodecurrency :: numeric,  
0 :: numeric as usd_amt,
round (a.amountincompanycodecurrency /d.factor * d.planratetousd,2) :: numeric as plan_rate_amt,
c.route_to_market,
a.PostingDate :: timestamp,
a.FiscalYear,
a.FiscalPeriod,
b.bd_billingdocumenttype as  revenuetype,
null as program_cd_rpt,
null as dir_indir_dtl,
null as gsp_coverage,
getdate()
from bi_fin_int.sap_inter_journalentry a
--left  join bi_fin_int.sap_inter_billingdocument b  on (a.accountingdocument = b.bd_accountingdocument)
left  join bi_fin_int.sap_inter_billingdocument b  on (a.accountingdocument = b.bd_accountingdocument and a.companycode = b.bd_companycode and a.fiscalyear = b.bd_fiscalyear) 
left  join bi_sf_int.sa_nmst_order c on (c.order_id = b.bd_yy1_sfdcorderid ) 
left  join bi_sf_int.sa_mst_order g on (g.order_number = c.order_id)
left  join bi_fin_int.sap_inter_fxrates d on (d.currencycode = a.companycodecurrency)
left  join bi_fin_int.sap_inter_BillingDocument_Item e on  (e.BD_Item_BillingDocument = b.bd_billingdocument)   
left  join bi_sf_int.sa_nmst_account f on (f.account_id = c.order_id)
where a.accountingdocumenttype = ''RV'';

get diagnostics lv_rec_count4:= row_count; 

---------------------------------deferredrevenue_rp-------------------------------
insert into  bi_onestream.deferredrevenue_rp
select 
category_type,
category_type_dtl,
market,
sales_team,
onestream_entity_cd,
product_line,
funct_currency_code,
source_system,
nvl(gl_amt_funct,0) as funct_amt,
nvl(gl_amt,0) as usd_amt,
nvl(std_conv_gl_amt,0) as plan_rate_amt,
null  as rtm,
suse_fy,
suse_fiscal_period,
revenue_type,
program_cd_rpt,
dir_indir_dtl,
null as gsp_coverage
from bi_rptprd_int.bi_rp_suse_int_recognized_revenue;

get diagnostics lv_rec_count5:= row_count; 

---------------------------------DeferredRevenue_new-------------------------------

insert into bi_onestream.DeferredRevenue_new
select 
distinct
null as category_type,
c.program,
c.bill_to_country,
f.sales_team,
a.companycode,
e.bd_item_material,
a.companycodecurrency, 
''SAPS4'' as source_system,
a.amountincompanycodecurrency :: numeric as funct_amt, 
0 :: numeric as usd_amt,
round (a.amountincompanycodecurrency /d.factor * d.planratetousd,2) :: numeric as plan_rate_amt,
c.route_to_market,
a.fiscalyear   as fiscal_year, 
a.fiscalperiod as fiscal_period,
b.bd_billingDocumentType as  revenuetype,
null as program_cd_rpt,
null as dir_indir_dtl,
null as gsp_coverage,
getdate()
from bi_fin_int.sap_inter_journalentry a
--left  join bi_fin_int.sap_inter_billingdocument b  on (a.accountingdocument = b.bd_accountingdocument)
left  join bi_fin_int.sap_inter_billingdocument b  on (a.accountingdocument = b.bd_accountingdocument and a.companycode = b.bd_companycode and a.fiscalyear = b.bd_fiscalyear) 
left  join bi_sf_int.sa_nmst_order c on (c.order_id = b.bd_yy1_sfdcorderid )
left  join bi_sf_int.sa_mst_order g on (g.order_number = c.order_id)
left  join bi_fin_int.sap_inter_fxrates d on (d.currencycode = a.companycodecurrency)
left  join bi_fin_int.sap_inter_BillingDocument_Item e on  (e.BD_Item_BillingDocument = b.bd_billingdocument)   
left  join bi_sf_int.sa_nmst_account f on (f.account_id = c.order_id)
where a.accountingdocumenttype  in (''Z1'',''RV'');

get diagnostics lv_rec_count6:= row_count; 

---------------------------------inperiodrevenue_rp-------------------------------

insert into  bi_onestream.inperiodrevenue_rp
select 
category_type,
category_type_dtl,
market,
sales_team,
onestream_entity_cd,
product_line,
funct_currency_code,
source_system,
sum(gl_amt_funct) :: numeric as funct_amt,
sum(gl_amt) :: numeric  as usd_amt,
sum (std_conv_gl_amt) :: numeric as plan_rate_amt,
null as rtm,
revenue_type,
program_cd_rpt,
dir_indir_dtl,
null as gsp_coverage,
getdate()
from bi_rptprd_int.bi_rp_suse_int_recognized_revenue 
where suse_fy =     case when to_char(sysdate,''mm'') :: numeric < 11  
                                then to_char(sysdate,''yyyy'') :: numeric
                                else  to_char(sysdate,''yyyy''):: numeric -1  
					end
and  suse_fiscal_period :: numeric <= mod(trunc(to_char(sysdate,''mm'')),12) :: numeric +2 :: numeric 
group by category_type,
category_type_dtl,
market,
sales_team,
onestream_entity_cd,
product_line,
funct_currency_code,
source_system,
revenue_type,
program_cd_rpt,
dir_indir_dtl;


get diagnostics lv_rec_count7:= row_count; 

---------------------------------InPeriodRevenue_new-------------------------------


insert into  bi_onestream.InPeriodRevenue_new
select 
distinct
null as category_type,
c.program,
c.bill_to_country,
f.sales_team,
a.companycode,
e.bd_item_material,
a.companycodecurrency, 
''SAPS4'' as source_system,
a.amountincompanycodecurrency :: numeric as funct_amt, 
0 :: numeric as usd_amt,
round (a.amountincompanycodecurrency /d.factor * d.planratetousd,2) :: numeric as plan_rate_amt,
c.route_to_market,
b.bd_billingDocumentType as  revenuetype,
null as program_cd_rpt,
null as dir_indir_dtl,
null as gsp_coverage,
getdate()
from bi_fin_int.sap_inter_journalentry a
--left  join bi_fin_int.sap_inter_billingdocument b  on (a.accountingdocument = b.bd_accountingdocument)
left  join bi_fin_int.sap_inter_billingdocument b  on (a.accountingdocument = b.bd_accountingdocument and a.companycode = b.bd_companycode and a.fiscalyear = b.bd_fiscalyear) 
left  join bi_sf_int.sa_nmst_order c on (c.order_id = b.bd_yy1_sfdcorderid ) 
left  join bi_sf_int.sa_mst_order g on (g.order_number = c.order_id)
left  join bi_fin_int.sap_inter_fxrates d on (d.currencycode = a.companycodecurrency)
left  join bi_fin_int.sap_inter_BillingDocument_Item e on (e.BD_Item_BillingDocument = b.bd_billingdocument)   
left  join bi_sf_int.sa_nmst_account f on (f.account_id = c.order_id) where a.accountingdocumenttype= ''Z1''
and a.fiscalyear =  case when to_char(sysdate,''mm'') :: numeric < 11  
                                then to_char(sysdate,''yyyy'') :: numeric
                                else  to_char(sysdate,''yyyy''):: numeric -1  
					    end
and  a.fiscalperiod :: numeric <= mod(trunc(to_char(sysdate,''mm'')),12) :: numeric +2 :: numeric
;


get diagnostics lv_rec_count8:= row_count;  

----------------------------------------------------------------------------------


lv_end_time := sysdate;

lv_rec_count := nvl(lv_rec_count1,0) + nvl(lv_rec_count2,0) + nvl(lv_rec_count3,0)+ nvl(lv_rec_count4,0)+ 
                nvl(lv_rec_count5,0)+ nvl(lv_rec_count6,0)+ nvl(lv_rec_count7,0)+ nvl(lv_rec_count8,0);


lv_table_name := ''bi_onestream.acv_rp'' ||'',''||''bi_onestream.acv_new'' ||'','' || ''bi_onestream.tcv_rp'' ||'',''
                 ||'' bi_onestream.tcv_new'' ||'',''||''bi_onestream.deferredrevenue_rp'' || '',''||''bi_onestream.deferredrevenue_new''|| '',''
				 || '' bi_onestream.inperiodrevenue_rp''|| '',''|| ''bi_onestream.inperiodrevenue_new'';



Insert into bi_exec_log values (lv_count,''ONESTREAM'',''bi_onestream_load'',lv_table_name,lv_error,''S'',nvl(lv_rec_count,0),lv_start_time,lv_end_time,sysdate,user);   


------------------------------------Creating Snapshots-----------------------------------------------
v_table_name := ''bi_onestream.acv_rp''||''_''||to_char(sysdate,''ddmmyyyyhhmiss'');
execute ''CREATE TABLE '' ||v_table_name||'' as Select * from ''|| ''bi_onestream.acv_rp'';

v_table_name := ''bi_onestream.acv_new''||''_''||to_char(sysdate,''ddmmyyyyhhmiss'');
execute ''CREATE TABLE ''||v_table_name||'' as Select * from ''|| ''bi_onestream.acv_new'';

v_table_name := ''bi_onestream.tcv_rp''||''_''||to_char(sysdate,''ddmmyyyyhhmiss'');
execute ''CREATE TABLE '' ||v_table_name||'' as Select * from ''|| ''bi_onestream.tcv_rp'';

v_table_name := ''bi_onestream.tcv_new''||''_''||to_char(sysdate,''ddmmyyyyhhmiss'');
execute ''CREATE TABLE '' ||v_table_name||''as Select * from ''|| ''bi_onestream.tcv_new'';

v_table_name := ''bi_onestream.deferredrevenue_rp''||''_''||to_char(sysdate,''ddmmyyyyhhmiss'');
execute ''CREATE TABLE '' ||v_table_name||''as Select * from ''|| ''bi_onestream.deferredrevenue_rp'';

v_table_name := ''bi_onestream.deferredrevenue_new''||''_''||to_char(sysdate,''ddmmyyyyhhmiss'');
execute ''CREATE TABLE '' ||v_table_name||'' as Select * from ''|| ''bi_onestream.deferredrevenue_new'';

v_table_name := ''bi_onestream.inperiodrevenue_rp''||''_''||to_char(sysdate,''ddmmyyyyhhmiss'');
execute ''CREATE TABLE '' ||v_table_name||''as Select * from ''|| ''bi_onestream.inperiodrevenue_rp'';

v_table_name := ''bi_onestream.inperiodrevenue_new''||''_''||to_char(sysdate,''ddmmyyyyhhmiss'');
execute ''CREATE TABLE ''||v_table_name||''as Select * from ''|| ''bi_onestream.inperiodrevenue_new'';

-----------------------------------------------------------------------------------------------------		
    

EXCEPTION WHEN OTHERS THEN 
      RAISE exception ''Exception message SQLERRM %'', SQLERRM ;
      RAISE exception ''Exception message SQLSTATE %'', SQLSTATE;
  
End;
'








 

