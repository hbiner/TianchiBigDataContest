

#####  brand_features_temp  ==>  brand_features      #################
sql("""
drop table if exists   lm_brand_features_5_13_7_08  ;
create table  lm_brand_features_5_13_7_08  as
select 
	brand_id,
	count(*)  asked_users ,
	sum(clicked) 	n_clicked,
	sum(bought)		n_bought,
	sum(collected)	n_collected,
	sum(basketed)	n_basketed,
	sum(actions)	actions_by 
from 
  lm_brand_features_5_13_7_08_temp
group by brand_id;

""")
######################################
