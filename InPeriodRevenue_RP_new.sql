create table if not exists bi_onestream.InPeriodRevenue_new
(
category_type varchar(255),
category_type_dtl varchar(255),
geography varchar(255),
sales_team varchar(255),
legal_entity_id_temp varchar(255),
product_line varchar(255),
funct_currency_code varchar(255),
source_system varchar(255),
funct_amt numeric(18,3),
usd_amt numeric (18,3),
plan_rate_amt numeric(18,3),
rtm varchar(255),
revenuetype varchar(255),
program_cd_rpt varchar(255),
dir_indir_dtl varchar(255),
gsp_coverage varchar(255),
system_created_date timestamp);



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
'SAPS4' as source_system,
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
left  join bi_fin_int.sap_inter_BillingDocument_Item e on  (e.BD_Item_BillingDocument = b.bd_billingdocument)   
left  join bi_sf_int.sa_nmst_account f on (f.account_id = c.order_id)where a.accountingdocumenttype  = 'Z1'
and a.fiscalyear =      case when to_char(sysdate,'mm') :: numeric < 11  
                                then to_char(sysdate,'yyyy') :: numeric
                                else  to_char(sysdate,'yyyy'):: numeric -1  
					    end
and  a.fiscalperiod :: numeric <= mod(trunc(to_char(sysdate,'mm')),12) :: numeric +2 :: numeric 
;

