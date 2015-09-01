# create  features table 6_11_8_15
#
#
sql("""
drop table if exists  lmn_u_or_b_features_6_11_8_15_extend ;
create table  lmn_u_or_b_features_6_11_8_15_extend  as
select 
	brand_id,
	user_id ,

	clicked ,
 	bought  ,
	collected ,
	basketed ,
	case
	when clicked > 0 then   1
	when clicked  = 0 then  0
	end as clicked_flag ,
	case
	when bought > 0 then	1  
	when bought  = 0 then   0
	end as bought_flag ,
	case
	when collected > 0 then	  1 
	when collected  = 0 then  0
	end as collected_flag ,
	case
	when basketed > 0 then   1
	when basketed  = 0 then  0
	end as basketed_flag ,

	actions 
from 
 lmn_u_or_b_features_6_11_8_15_temp
""")

#####  brand_features_extend  ==>  brand_features      #################
sql("""
drop table if exists  lmn_brand_features_6_11_8_15_t ;
create table  lmn_brand_features_6_11_8_15_t  as
select 
	brand_id,
	cast( count(*) as double)  users_dist ,
	sum(clicked) 	n_clicked,
	sum(bought)		n_bought,
	sum(collected)	n_collected,
	sum(basketed)	n_basketed,
	sum(actions)	actions_by ,

	cast( sum(clicked_flag) as double) 		clicked_dist,
	cast( sum(bought_flag)	as double)		bought_dist,
	cast( sum(collected_flag) as double)	collected_dist,
	cast( sum(basketed_flag) as double)		basketed_dist

from 
lmn_u_or_b_features_6_11_8_15_extend
group by brand_id;
""")
######################################
##  brand_features_extend == > user_features 
sql("""
drop table if exists  lmn_user_features_6_11_8_15_t ;
create table  lmn_user_features_6_11_8_15_t  as
select 
	user_id,
	cast( count(*) as double)  brand_dist ,
	sum(clicked) 	n_click,
	sum(bought)	n_buy,
	sum(collected)	n_collect,
	sum(basketed)	n_basket,
	sum(actions)	actions_sum ,

	cast( sum(clicked_flag) as double) 		click_dist,
	cast( sum(bought_flag) as double)		buy_dist,
	cast( sum(collected_flag) as double)	collect_dist,
	cast( sum(basketed_flag) as double)		basket_dist

from 
lmn_u_or_b_features_6_11_8_15_extend
group by user_id;
""")

##
## table 1 , brand features

sql("""
drop table if exists lmn_brand_features_6_11_8_15 ;
create table  lmn_brand_features_6_11_8_15 as
select 
      brand_id , 
 	n_clicked/actions_by 	n_clicked_ratio,
	n_bought/actions_by	n_bought_ratio,
	n_collected/actions_by	n_collected_ratio,	
	n_basketed/actions_by	n_basketed_ratio,	
	actions_by ,
	clicked_dist/users_dist 		clicked_dist_ratio,
	bought_dist/users_dist 			bought_dist_ratio,
	collected_dist/users_dist		collected_dist_ratio,
	basketed_dist/users_dist		basketed_dist_ratio ,
      users_dist
from
lmn_brand_features_6_11_8_15_t
""")
######
# table 2 , user features 
sql("""
drop table if exists lmn_user_features_6_11_8_15 ;
create table  lmn_user_features_6_11_8_15 as
select 
      user_id , 
 	n_click/actions_sum		n_click_ratio,
	n_buy/actions_sum			n_buy_ratio,
	n_collect/actions_sum		n_collect_ratio ,
	n_basket/actions_sum		n_basket_ratio,
	actions_sum ,

	click_dist/brand_dist		click_dist_ratio,
	buy_dist/brand_dist		buy_dist_ratio,
	collect_dist/brand_dist		collect_dist_ratio,
	basket_dist/brand_dist 		basket_dist_ratio,
	brand_dist
from 
lmn_user_features_6_11_8_15_t
""")
###
#  table 3 , user_brand features 
## 
sql("""
drop table if exists lmn_user_brand_features_6_11_8_15 ;
create table  lmn_user_brand_features_6_11_8_15 as
select 
      user_id , 
	brand_id ,
		click1	 ,
	click3	 ,
	click5	 ,
	click7	 ,
	click10	,
	click15	 ,
	click25	 ,
	click26	 ,

	buy1	 ,
	buy3	 ,
	buy5	 ,
	buy7	 ,
	buy10	,
	buy15	 ,
	buy25	 ,
	buy26 ,

	collect1	 ,
	collect3	 ,
	collect5	 ,
	collect7	 ,
	collect10	,
	collect15	 ,
	collect25	 ,
	collect26	 ,

	basket1	 ,
	basket3	 ,
	basket5	 ,
	basket7	 ,
	basket10	,
	basket15	 ,
	basket25	,
	basket26	,
      
      click_this/action_this	  click_this_ratio	,
	buy_this /action_this    buy_this_ratio	,
	collect_this /action_this   collect_this_ratio	 ,
	basket_this	/action_this    basket_this_ratio	,
	action_this	,
	last_datetime		
from 
lmn_user_brand_features_6_11_8_15_t
""")

