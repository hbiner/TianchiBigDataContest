# create train table :	lmn_train_area_submit_5_11_8_15
# create predict table :	lmn_predict_area_submit_6_11_8_15

####
#   create   train table 
###

#connect all ,get train table for submit
sql("""
drop table if  exists lmn_train_area_submit_5_11_8_15_udf  ;
create table lmn_train_area_submit_5_11_8_15_udf as
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
 	clicked_ratio1  ,
  	clicked_ratio3 ,
  	clicked_ratio5 ,
  	clicked_ratio7 ,
	clicked_ratio10 ,
  	clicked_ratio15 ,
  	clicked_ratio25 ,

       bought_ratio1  ,
       bought_ratio3 ,
   	  bought_ratio5 ,
       bought_ratio7 ,
       bought_ratio10 ,
	bought_ratio15 ,
       bought_ratio25 ,

 collected_ratio1  ,
 collected_ratio3 ,
  collected_ratio5 ,
  collected_ratio7 ,
  collected_ratio10 ,
collected_ratio15 ,
  collected_ratio25 ,

 basketed_ratio1  ,
  basketed_ratio3 ,
  basketed_ratio5 ,
  basketed_ratio7 ,
  basketed_ratio10 ,
basketed_ratio15 ,
  basketed_ratio25 ,
 
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
lmn_brand_features_5_11_7_15_bdf
on 	lmn_user_brand_features_5_11_7_15.brand_id = lmn_brand_features_5_11_7_15_bdf.brand_id		

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



###########
#
# create 	predict table   
#
###########

#connect all ,get train table for validate
sql("""
drop table if  exists lmn_predict_area_submit_6_11_8_15_udf;
create table lmn_predict_area_submit_6_11_8_15_udf as
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

 	clicked_ratio1  ,
  	clicked_ratio3 ,
  	clicked_ratio5 ,
  	clicked_ratio7 ,
	clicked_ratio10 ,
  	clicked_ratio15 ,
  	clicked_ratio25 ,

       bought_ratio1  ,
       bought_ratio3 ,
   	  bought_ratio5 ,
       bought_ratio7 ,
       bought_ratio10 ,
	bought_ratio15 ,
       bought_ratio25 ,

 collected_ratio1  ,
 collected_ratio3 ,
  collected_ratio5 ,
  collected_ratio7 ,
  collected_ratio10 ,
collected_ratio15 ,
  collected_ratio25 ,

 basketed_ratio1  ,
  basketed_ratio3 ,
  basketed_ratio5 ,
  basketed_ratio7 ,
  basketed_ratio10 ,
basketed_ratio15 ,
  basketed_ratio25 ,




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
lmn_brand_features_6_11_8_15_bdf
on 	lmn_user_brand_features_6_11_8_15.brand_id = lmn_brand_features_6_11_8_15_bdf.brand_id		

where actions_sum is not null and  actions_by is not null   ; --fliter
""")

