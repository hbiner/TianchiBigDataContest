# 1 create  lm_train_area_validate_4_20_7_15_v0_0order
sql("""
drop table if  exists lm_train_area_validate_4_20_7_15_v0_0order  ;
create table lm_train_area_validate_4_20_7_15_v0_0order as
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
	ask_brands/n_actions    p_askbrands,
      n_click/n_actions 	p_click,
	n_buy/n_actions 		p_buy,
	n_collect/n_actions 	p_collect,
	n_basket/n_actions 	p_basket ,


	asked_users ,
   	n_clicked,
 	n_bought,
	n_collected,
	n_basketed,
	actions_by,
	asked_users/actions_by  p_askedusers,
	n_clicked/actions_by	p_clicked,
	n_bought/actions_by	p_bought,
	n_collected/actions_by	p_collected,
	n_basketed/actions_by	p_basketed ,

	buy_label
 
from
 lm_train_area_validate_4_20_7_15_v0

""")


# 2  create   lm_train_area_validate_4_20_7_15_v0_0order 
sql("""
drop table if  exists lm_train_area_validate_4_20_7_15_v0_0order  ;
create table lm_train_area_validate_4_20_7_15_v0_0order as
select 
	lm_train_area_validate_4_20_7_15_v0_00order.user_id,
      lm_train_area_validate_4_20_7_15_v0_00order.brand_id,
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
	p_askbrands,
     	p_click,
	p_buy,
	p_collect,
	p_basket ,
      n_click*p_click+n_buy*p_buy+n_collect*p_collect+n_basket*p_basket  user_m ,


	asked_users ,
   	n_clicked,
 	n_bought,
	n_collected,
	n_basketed,
	actions_by,
	p_askedusers,
	p_clicked,
	p_bought,
	p_collected,
	p_basketed ,
  	n_clicked*p_clicked+n_bought*p_bought+n_collected*p_collected+n_basketed*p_basketed     brand_m ,

	buy_label
 
from
 lm_train_area_validate_4_20_7_15_v0_00order

""")



# 3  create   lm_train_area_validate_4_20_7_15_v0_1order 
sql("""
drop table if  exists lm_train_area_validate_4_20_7_15_v0_1order  ;
create table lm_train_area_validate_4_20_7_15_v0_1order as
select 
	lm_train_area_validate_4_20_7_15_v0_0order.user_id,
      lm_train_area_validate_4_20_7_15_v0_0order.brand_id,
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
	p_askbrands,
     	p_click,
	p_buy,
	p_collect,
	p_basket ,
      user_m ,
	pow( n_click - user_m ,1)*p_click +pow( n_buy - user_m ,1)*p_buy
	+pow( n_collect - user_m ,1)*p_collect+pow( n_basket - user_m ,1)*p_basket  u_1order ,
	pow( n_click - user_m ,2)*p_click +pow( n_buy - user_m ,2)*p_buy
	+pow( n_collect - user_m ,2)*p_collect+pow( n_basket - user_m ,2)*p_basket  u_2order ,


	asked_users ,
   	n_clicked,
 	n_bought,
	n_collected,
	n_basketed,
	actions_by,
	p_askedusers,
	p_clicked,
	p_bought,
	p_collected,
	p_basketed ,
  	 brand_m ,
	pow( n_clicked - user_m ,1)*p_clicked +pow( n_bought - user_m ,1)*p_bought
	+pow( n_collected - user_m ,1)*p_collected+pow( n_basketed - user_m ,1)*p_basketed  b_1order ,
	pow( n_clicked - user_m ,2)*p_clicked +pow( n_bought - user_m ,2)*p_bought
	+pow( n_collected - user_m ,2)*p_collected+pow( n_basketed - user_m ,2)*p_basketed  b_2order ,

	buy_label
 
from
 lm_train_area_validate_4_20_7_15_v0_0order

""")



