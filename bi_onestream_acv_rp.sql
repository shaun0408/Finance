create table if not exists bi_onestream.acv_rp
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
fiscal_year varchar(255),
fiscal_period varchar(255),
revenuetype varchar(255),
program_cd_rpt varchar(255),
dir_indir_dtl varchar(255),
gsp_coverage varchar(255),
system_created_date timestamp);


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
