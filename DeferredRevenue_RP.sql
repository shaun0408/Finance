create table if not exists bi_onestream.deferredrevenue_rp
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
fiscal_year varchar(255),
fiscal_period varchar(255),
revenuetype varchar(255),
program_cd_rpt varchar(255),
dir_indir_dtl varchar(255),
gsp_coverage varchar(255),
system_created_date timestamp);

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

