
DataProc.appendColumns (['lmn_user_features_4_11_6_15_t',
				'lmn_u_class_4_11_6_15_indc'],
				'lmn_user_features_4_11_6_15_t_addclass')

DataProc.appendColumns (['lmn_brand_features_4_11_6_15_t',
				'lmn_b_class_4_11_6_15_indc'],
				'lmn_brand_features_4_11_6_15_t_addclass')

DataProc.appendColumns (['lmn_user_features_5_11_7_15_t',
				'lmn_u_class_5_11_7_15_indc'],
				'lmn_user_features_5_11_7_15_t_addclass')

DataProc.appendColumns (['lmn_brand_features_5_11_7_15_t',
				'lmn_b_class_5_11_7_15_indc'],
				'lmn_brand_features_5_11_7_15_t_addclass')


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
	brand_dist   ,
	cluster_index 			u_class
	
from 
lmn_user_features_4_11_6_15_t_addclass
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
       users_dist  ,
	cluster_index			b_class		
from
lmn_brand_features_4_11_6_15_t_addclass
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
	brand_dist   ,
	cluster_index 			u_class
from 
lmn_user_features_5_11_7_15_t_addclass
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
       users_dist  ,
	cluster_index			b_class
from
lmn_brand_features_5_11_7_15_t_addclass
""")
####





# connect 

sql("""
drop table if  exists lmn_train_area_validate_4_11_7_15_addclass  ;
create table lmn_train_area_validate_4_11_7_15_addclass as
select
	lmn_user_brand_features_4_11_6_15.user_id,
      lmn_user_brand_features_4_11_6_15.brand_id,
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
	 action_this	,

 -- brand features
      --
  	n_clicked_ratio,
	n_bought_ratio,
	n_collected_ratio,	
	n_basketed_ratio,	
	actions_by ,

	clicked_dist_ratio,
	bought_dist_ratio,
	collected_dist_ratio,
	basketed_dist_ratio ,
       users_dist ,
	lmn_brand_features_4_11_6_15.b_class,

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
      brand_dist  ,
      lmn_user_features_4_11_6_15.u_class ,	

	last_datetime ,		

      --label
      buy_brand	,
	case
	when buy_brand is not null then 1
	when buy_brand is null then 0
	end as buy_label

from
 lmn_user_brand_features_4_11_6_15

left outer join
lmn_user_features_4_11_6_15
on 	lmn_user_brand_features_4_11_6_15.user_id = lmn_user_features_4_11_6_15.user_id

left outer join
lmn_brand_features_4_11_6_15
on 	lmn_user_brand_features_4_11_6_15.brand_id = lmn_brand_features_4_11_6_15.brand_id		

left outer join(
   select
   user_id,
   brand_id  buy_brand
from(
	select user_id,brand_id 
	from t_alibaba_bigdata_user_brand_total_1
	where type = '1' and  visit_datetime>='06-16' and   visit_datetime <= '07-15'
	group by  user_id , brand_id
    )label
) buy_label_table_6_16_7_15
on 	lmn_user_brand_features_4_11_6_15.user_id = buy_label_table_6_16_7_15.user_id and 	lmn_user_brand_features_4_11_6_15.brand_id = buy_label_table_6_16_7_15.buy_brand

where actions_sum is not null and  actions_by is not null   ; --fliter
""")


############################################################################################################################################################

#connect all ,get train table for validate
sql("""
drop table if  exists lmn_predict_area_validate_5_11_7_15_addclass  ;
create table lmn_predict_area_validate_5_11_7_15_addclass as
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
	 action_this	,

 -- brand features
      --
  	n_clicked_ratio,
	n_bought_ratio,
	n_collected_ratio,	
	n_basketed_ratio,	
	actions_by ,

	clicked_dist_ratio,
	bought_dist_ratio,
	collected_dist_ratio,
	basketed_dist_ratio ,
       users_dist ,
	lmn_brand_features_5_11_7_15.b_class,

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
      brand_dist  ,
      lmn_user_features_5_11_7_15.u_class ,	

	last_datetime 		
from
 lmn_user_brand_features_5_11_7_15

left outer join
lmn_user_features_5_11_7_15
on 	lmn_user_brand_features_5_11_7_15.user_id = lmn_user_features_5_11_7_15.user_id

left outer join
lmn_brand_features_5_11_7_15
on 	lmn_user_brand_features_5_11_7_15.brand_id = lmn_brand_features_5_11_7_15.brand_id	
where actions_sum is not null and  actions_by is not null   ; --fliter
""")




