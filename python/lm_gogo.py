

sql("""
-- 
drop table if exists lm_user_fre_value_all ;
create table lm_user_fre_value_all as
select 
  user_id,
  sum(n_click)+sum(n_buy)+sum(n_collect)+sum(n_basket) as actions,
  sum(n_click)/(sum(n_click)+sum(n_buy)+sum(n_collect)+sum(n_basket)) as fre_click,
  sum(n_buy)/(sum(n_click)+sum(n_buy)+sum(n_collect)+sum(n_basket)) as fre_buy
 from 
  yi_user_brand_information 
  group by user_id;
""")



sql("""
-- connect 
drop table if exists lm_user_brand_fre_for_mr_all ;
create table lm_user_brand_fre_for_mr_all as
select
lm_small_sample_of_u_b_information.user_id,
lm_small_sample_of_u_b_information.brand_id,
n_click, n_buy, n_collect, n_basket,
fre_click,
fre_buy,
actions
from
lm_small_sample_of_u_b_information 
left outer join
lm_user_fre_value
on lm_small_sample_of_u_b_information.user_id = lm_user_fre_value.user_id 
where actions >=7 ;-- fliter
""")


sql("""
create table lm_score_output_all
(
     user_id string,
    brand_id string,
     score double
)
""")