create table if not exists bi_onestream.tcv_new
(
geography varchar(255),
sales_team varchar(255),
legal_entity_id_temp varchar(255),
product_line varchar(255),
funct_currency_code varchar(255),
source_system varchar(255),
funct_amt numeric(18,3),
usd_amt numeric(18,3),
plan_rate_amt numeric(18,3),
rtm varchar(255),
trx_month timestamp,
fiscal_year varchar(255),
fiscal_period varchar(255),
revenuetype varchar(255),
program_cd_rpt varchar(255),
dir_indir_dtl varchar(255),
gsp_coverage varchar(255),
system_created_date timestamp);


insert into bi_onestream.tcv_new
select   
distinct
c.bill_to_country,
f.sales_team,
a.companycode,
e.bd_item_material,
a.companycodecurrency, 
'SAP4' as source_system,
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
where a.accountingdocumenttype = 'RV' ;











