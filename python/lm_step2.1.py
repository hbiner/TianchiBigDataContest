
#####
sql("""
drop table if exists lmn_user_features_4_11_6_15 ;
create table  lmn_user_features_4_11_6_15 as
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
	cast ( brand_dist as double)  brand_dist 
	
from 
lmn_user_features_4_11_6_15_t
""")

######   ==>  4_11_6_15
sql("""
drop table if exists  lmn_brand_features_4_11_6_15 ;
create table  lmn_brand_features_4_11_6_15 as
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
      cast( users_dist as double)  users_dist
from
lmn_brand_features_4_11_6_15_t
""")
####

##
sql("""
drop table if exists   lmn_user_features_5_11_7_15 ;
create table  lmn_user_features_5_11_7_15 as
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
	cast( brand_dist as double)	brand_dist
from 
lmn_user_features_5_11_7_15_t
""")
####
######   ==>  5_11_7_15
sql("""
drop table if exists  lmn_brand_features_5_11_7_15 ;
create table  lmn_brand_features_5_11_7_15 as
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
      cast( users_dist as double)  users_dist
from
lmn_brand_features_5_11_7_15_t
""")
####
sql("""
drop table if exists lmn_user_brand_features_5_11_7_15 ;
create table  lmn_user_brand_features_5_11_7_15 as
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

	buy1	 ,
	buy3	 ,
	buy5	 ,
	buy7	 ,
	buy10	,
	buy15	 ,
	buy25	 ,

	collect1	 ,
	collect3	 ,
	collect5	 ,
	collect7	 ,
	collect10	,
	collect15	 ,
	collect25	 ,

	basket1	 ,
	basket3	 ,
	basket5	 ,
	basket7	 ,
	basket10	,
	basket15	 ,
	basket25	,
      
      click_this/action_this	  click_this_ratio	,
	buy_this /action_this    buy_this_ratio	,
	collect_this /action_this   collect_this_ratio	 ,
	basket_this	/action_this    basket_this_ratio	,
	action_this	,
	last_datetime		
from 
lmn_user_brand_features_5_11_7_15_t
""")
##  end of  ==> 5_11_7_15_t ratio
###################################################
###################################################
######   ==>  4_11_6_15

sql("""
drop table if exists lmn_user_brand_features_4_11_6_15 ;
create table  lmn_user_brand_features_4_11_6_15 as
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

	buy1	 ,
	buy3	 ,
	buy5	 ,
	buy7	 ,
	buy10	,
	buy15	 ,
	buy25	 ,

	collect1	 ,
	collect3	 ,
	collect5	 ,
	collect7	 ,
	collect10	,
	collect15	 ,
	collect25	 ,

	basket1	 ,
	basket3	 ,
	basket5	 ,
	basket7	 ,
	basket10	,
	basket15	 ,
	basket25	,
      
      click_this/action_this	  click_this_ratio	,
	buy_this /action_this    buy_this_ratio	,
	collect_this /action_this   collect_this_ratio	 ,
	basket_this	/action_this    basket_this_ratio	,
	action_this	,
	last_datetime		
from 
lmn_user_brand_features_4_11_6_15_t
""")

##### 

sql("""
create table  lmn_user_brand_features_4_11_6_15 as
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

	buy1	 ,
	buy3	 ,
	buy5	 ,
	buy7	 ,
	buy10	,
	buy15	 ,
	buy25	 ,

	collect1	 ,
	collect3	 ,
	collect5	 ,
	collect7	 ,
	collect10	,
	collect15	 ,
	collect25	 ,

	basket1	 ,
	basket3	 ,
	basket5	 ,
	basket7	 ,
	basket10	,
	basket15	 ,
	basket25	,
      
      click_this/action_this	  click_this_ratio	,
	buy_this /action_this    buy_this_ratio	,
	collect_this /action_this   collect_this_ratio	 ,
	basket_this	/action_this    basket_this_ratio	,
	action_this	,
	last_datetime		
from 
lmn_user_brand_features_4_11_6_15_t


""")
##  end of  ==>  4_11_6_15

##  extend block 








