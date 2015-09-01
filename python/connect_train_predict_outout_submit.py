
#connect all
sql("""
drop table if  exists  ;
create table lm_lr_train_5_25_8_15_t as
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
	n_actions	  ,
	buy_brand	,
	case
	when buy_brand is not null then 1
	when buy_brand is null then 0
	end as buy_label
from
 lm_user_brand_features_5_25_7_15
left outer join
lm_user_features_5_25_7_15
on 	lm_user_brand_features_5_25_7_15.user_id = lm_user_features_5_25_7_15.user_id	

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
on 	lm_user_brand_features_5_25_7_15.user_id = buy_label_table_7_16_8_15.user_id and 	lm_user_brand_features_5_25_7_15.brand_id = buy_label_table_7_16_8_15.buy_brand

where n_actions is not null   ; --fliter
""")

# output model : lm_lr_train_5_25_8_15_model

#########################################################################################

#a model :lm_lr_train_5_25_8_15_model
#input table  lm_lr_predict_area 
##############     predict   base on tabel lm_lr_predict_area  with model       #########

#########################################################################################
# input table :  lm_lr_prediction_output
# output table :t_tmall_add_user_brand_predict_dh
sql("""
drop table if exists t_tmall_add_user_brand_predict_dh ; --
create table t_tmall_add_user_brand_predict_dh as  --
select --  first
    user_id, 
    wm_concat(',',brand_id) as brand -- connect the result
from
   ( select
	user_id ,
   	brand_id 
  from lm_lr_prediction_output
	where probability >= 0.0183     -- set the threshold value --    276 9080
) a
  
group by user_id;  
""")


###  random tree 
sql("""
drop table if exists t_tmall_add_user_brand_predict_dh ; --
create table t_tmall_add_user_brand_predict_dh as  --
select --  first
    user_id, 
    wm_concat(',',brand_id) as brand -- connect the result
from
   ( select
	user_id ,
   	brand_id 
  from lm_train_area_submit_5_20_8_15_rf_model_1000trees_output
	where probability >= 0.0207     -- set the threshold value -- 
) a
  
group by user_id;  
""")






