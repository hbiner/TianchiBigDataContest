#connect all
sql("""
drop table if  exists lm_lr_train_4_25_7_15 ;
create table lm_lr_train_4_25_7_15 as
select 
	lm_user_brand_feature_4_25_6_15.user_id,
      lm_user_brand_feature_4_25_6_15.brand_id,
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
 lm_user_brand_feature_4_25_6_15
left outer join
lm_user_feature_4_25_6_15
on 	lm_user_brand_feature_4_25_6_15.user_id = lm_user_feature_4_25_6_15.user_id	

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
) label_table_6_16_7_15
on 	lm_user_brand_feature_4_25_6_15.user_id = label_table_6_16_7_15.user_id and 	lm_user_brand_feature_4_25_6_15.brand_id = label_table_6_16_7_15.buy_brand

where n_actions is not null  ;
""")


#########################################################################################

##############     predict        ####################################

#########################################################################################
# output 
sql("""
drop table if exists lm_t_tmall_add_user_brand_predict_dh ; --
create table lm_t_tmall_add_user_brand_predict_dh as  --
select --  first
    user_id, 
    wm_concat(',',brand_id) as brand -- connect the result
from
   ( select
	user_id ,
   	brand_id 
  from lm_lr_prediction_output
	where probability >= 0.05
) a
  
group by user_id;  
""")








