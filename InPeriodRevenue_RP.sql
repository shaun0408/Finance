create table if not exists bi_onestream.inperiodrevenue_rp
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
where suse_fy =     case when to_char(sysdate,'mm') :: numeric < 11  
                                then to_char(sysdate,'yyyy') :: numeric
                                else  to_char(sysdate,'yyyy'):: numeric -1  
					end
and  suse_fiscal_period :: numeric <= mod(trunc(to_char(sysdate,'mm')),12) :: numeric +2 :: numeric 
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
