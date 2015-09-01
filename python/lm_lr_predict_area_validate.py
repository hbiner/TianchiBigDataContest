#connect all
sql("""
drop table if  exists  lm_lr_predict_area_validate;
create table  lm_lr_predict_area_validate as
select 
	lm_user_brand_features_5_25_7_15.user_id,
      lm_user_brand_features_5_25_7_15.brand_id,
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
	n_actions	  

from
 lm_user_brand_features_5_25_7_15
left outer join
lm_user_features_4_15_7_15
on 	lm_user_brand_features_5_25_7_15.user_id = lm_user_features_4_15_7_15.user_id
	
where n_click is not null and ; --fliter
""")



