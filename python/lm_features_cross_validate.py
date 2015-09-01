#  features cross validate 

sql("""
drop table if  exists lm_train_area_validate_4_20_7_15_v0_cross  ;
create table lm_train_area_validate_4_20_7_15_v0_cross as
select 
	lm_train_area_validate_4_20_7_15_v0.user_id,
      lm_train_area_validate_4_20_7_15_v0.brand_id,
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
      n_click * n_buy  cro_clickbuy,
	n_click * n_collect cro_clickcollect,
	n_click * n_basket  cro_clickbasket,
	n_buy * n_collect	cro_buycollect,
	n_buy * n_basket	cro_buybasket,
	n_collect * n_basket cro_collectbasket,

	asked_users ,
   	n_clicked,
 	n_bought,
	n_collected,
	n_basketed,
	actions_by,
	n_clicked * n_bought  cro_clickedbought,
	n_clicked * n_collected cro_clickedcollected,
	n_clicked * n_basketed  cro_clickedbasketed,
	n_bought * n_collected	cro_boughtcollected,
	n_bought * n_basketed	cro_boughtbasketed,
	n_collected * n_basketed cro_collectedbasketed,

	buy_label
 
from
 lm_train_area_validate_4_20_7_15_v0

""")

#####################


######################


sql("""
drop table if  exists lm_predict_area_validate_5_20_7_15_v0_cross  ;
create table lm_predict_area_validate_5_20_7_15_v0_cross as
select 
	lm_predict_area_validate_5_20_7_15_v0.user_id,
      lm_predict_area_validate_5_20_7_15_v0.brand_id,
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
      n_click * n_buy  cro_clickbuy,
	n_click * n_collect cro_clickcollect,
	n_click * n_basket  cro_clickbasket,
	n_buy * n_collect	cro_buycollect,
	n_buy * n_basket	cro_buybasket,
	n_collect * n_basket cro_collectbasket,

	asked_users ,
   	n_clicked,
 	n_bought,
	n_collected,
	n_basketed,
	actions_by,
	n_clicked * n_bought  cro_clickedbought,
	n_clicked * n_collected cro_clickedcollected,
	n_clicked * n_basketed  cro_clickedbasketed,
	n_bought * n_collected	cro_boughtcollected,
	n_bought * n_basketed	cro_boughtbasketed,
	n_collected * n_basketed cro_collectedbasketed
 
from
 lm_predict_area_validate_5_20_7_15_v0

""")





