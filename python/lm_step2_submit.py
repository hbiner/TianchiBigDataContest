 MR ...

#connect all ,get train table for submit
sql("""
drop table if  exists lmn_train_area_submit_5_11_8_15  ;
create table lmn_train_area_submit_5_11_8_15 as
select 
	lmn_user_brand_features_5_11_7_15.user_id,
      lmn_user_brand_features_5_11_7_15.brand_id,
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
      
      click_this_ratio	,
	buy_this_ratio	,
      collect_this_ratio	 ,
      basket_this_ratio	,
	action_this		,

      -- brand features
 	n_clicked_ratio,
	n_bought_ratio,
	n_collected_ratio,	
	n_basketed_ratio,	
	actions_by ,

	clicked_dist_ratio,
	bought_dist_ratio,
	collected_dist_ratio,
	basketed_dist_ratio ,
      users_dist,
 
	--user features 
	n_click_ratio,
	n_buy_ratio,
	n_collect_ratio ,
	n_basket_ratio,
	actions_sum ,

	click_dist_ratio,
	buy_dist_ratio,
	collect_dist_ratio,
	basket_dist_ratio,
	brand_dist,
	
	last_datetime ,
	--label
      buy_brand	,
	case
	when buy_brand is not null then 1
	when buy_brand is null then 0
	end as buy_label
from
 lmn_user_brand_features_5_11_7_15

left outer join
lmn_user_features_5_11_7_15
on 	lmn_user_brand_features_5_11_7_15.user_id = lmn_user_features_5_11_7_15.user_id

left outer join
lmn_brand_features_5_11_7_15
on 	lmn_user_brand_features_5_11_7_15.brand_id = lmn_brand_features_5_11_7_15.brand_id		

left outer join(
   select
   user_id,
   brand_id  buy_brand
from(
	select user_id,brand_id 
	from t_alibaba_bigdata_user_brand_total_1
	where type = '1' and  visit_datetime>='07-16' and   visit_datetime <= '08-15'
	group by  user_id , brand_id
    )label
) buy_label_table_7_16_8_15
on 	lmn_user_brand_features_5_11_7_15.user_id = buy_label_table_7_16_8_15.user_id and 	lmn_user_brand_features_5_11_7_15.brand_id = buy_label_table_7_16_8_15.buy_brand
where actions_sum is not null and  actions_by is not null   ; --fliter
""")

############################################################################################################################################################

################################################################################################################################################################################
##   brand_featers_temp  == > brand_features_extend  ###

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
	count(*)  users_dist ,
	sum(clicked) 	n_clicked,
	sum(bought)		n_bought,
	sum(collected)	n_collected,
	sum(basketed)	n_basketed,
	sum(actions)	actions_by ,

	sum(clicked_flag) 	clicked_dist,
	sum(bought_flag)		bought_dist,
	sum(collected_flag)	collected_dist,
	sum(basketed_flag)	basketed_dist

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
	count(*)  brand_dist ,
	sum(clicked) 	n_click,
	sum(bought)		n_buy,
	sum(collected)	n_collect,
	sum(basketed)	n_basket,
	sum(actions)	actions_sum ,

	sum(clicked_flag) 	click_dist,
	sum(bought_flag)		buy_dist,
	sum(collected_flag)	collect_dist,
	sum(basketed_flag)	basket_dist

from 
lmn_u_or_b_features_6_11_8_15_extend
group by user_id;
""")
######   ==>  ratio

sql("""
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
sql("""
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

sql("""
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
lmn_user_brand_features_6_11_8_15_t
""")
##  end of  ==>  ratio




#connect all ,get train table for validate
sql("""
drop table if  exists lmn_predict_area_validate_6_11_8_15;
create table lmn_predict_area_validate_6_11_8_15 as
select 
	lmn_user_brand_features_6_11_8_15.user_id,
      lmn_user_brand_features_6_11_8_15.brand_id,
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
      
      click_this_ratio	,
	buy_this_ratio	,
      collect_this_ratio	 ,
      basket_this_ratio	,
	action_this		,

      -- brand features
 	n_clicked_ratio,
	n_bought_ratio,
	n_collected_ratio,	
	n_basketed_ratio,	
	actions_by ,

	clicked_dist_ratio,
	bought_dist_ratio,
	collected_dist_ratio,
	basketed_dist_ratio ,
      users_dist,
 
	--user features 
	n_click_ratio,
	n_buy_ratio,
	n_collect_ratio ,
	n_basket_ratio,
	actions_sum ,

	click_dist_ratio,
	buy_dist_ratio,
	collect_dist_ratio,
	basket_dist_ratio,
	brand_dist,
	
	last_datetime	
from
 lmn_user_brand_features_6_11_8_15

left outer join
lmn_user_features_6_11_8_15
on 	lmn_user_brand_features_6_11_8_15.user_id = lmn_user_features_6_11_8_15.user_id

left outer join
lmn_brand_features_6_11_8_15
on 	lmn_user_brand_features_6_11_8_15.brand_id = lmn_brand_features_6_11_8_15.brand_id		

where actions_sum is not null and  actions_by is not null   ; --fliter
""")