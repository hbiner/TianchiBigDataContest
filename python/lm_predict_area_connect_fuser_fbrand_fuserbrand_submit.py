
#connect all
sql("""
drop table if  exists lm_predict_area_submit_6_20_8_15  ;
create table   lm_predict_area_submit_6_20_8_15 as
select 
	lm_user_brand_features_6_20_8_15.user_id,
      lm_user_brand_features_6_20_8_15.brand_id,
	click1	 ,
	click3	 ,
	click5	 ,
	click7	 ,
	click15	 ,
	click25	 ,

	buy1	 ,
	buy3	 ,
	buy5	 ,
	buy7	 ,
	buy15	 ,
	buy25	 ,

	collect1	 ,
	collect3	 ,
	collect5	 ,
	collect7	 ,
	collect15	 ,
	collect25	 ,

	basket1	 ,
	basket3	 ,
	basket5	 ,
	basket7	 ,
	basket15	 ,
	basket25	,

	ask_brands   ,
	n_click	 ,
	n_buy		 ,
	n_collect	 ,
	n_basket	 ,
	n_actions	  ,

      asked_users ,
   	n_clicked,
 	n_bought,
	n_collected,
	n_basketed,
	actions_by 
  
  
from
 lm_user_brand_features_6_20_8_15

left outer join
lm_user_features_6_20_8_15
on 	lm_user_brand_features_6_20_8_15.user_id = lm_user_features_6_20_8_15.user_id

left outer join
lm_brand_features_6_20_8_15
on 	lm_user_brand_features_6_20_8_15.brand_id = lm_brand_features_6_20_8_15.brand_id		

where n_actions is not null and  actions_by is not null   ; --fliter
""")
