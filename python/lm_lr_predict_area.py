#connect all
sql("""
drop table if  exists lm_lr_predict_area;
create table lm_lr_predict_area  as
select 
	lm_user_brand_features_6_25_8_15.user_id,
      lm_user_brand_features_6_25_8_15.brand_id,
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
 lm_user_brand_features_6_25_8_15
left outer join
lm_user_features_6_25_8_15
on 	lm_user_brand_features_6_25_8_15.user_id = lm_user_features_6_25_8_15.user_id	


where n_actions is not null ; --fliter
""")
