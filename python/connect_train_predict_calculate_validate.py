#####  brand_features_temp  ==>  brand_features      #################
sql("""
drop table if exists  lm_brand_features_4_20_6_15 ;
create table  lm_brand_features_4_20_6_15  as
select 
	brand_id,
	count(*)  asked_users ,
	sum(clicked) 	n_clicked,
	sum(bought)		n_bought,
	sum(collected)	n_collected,
	sum(basketed)	n_basketed,
	sum(actions)	actions_by 
from 
 lm_brand_features_4_20_6_15_temp
group by brand_id;

""")
######################################


#connect all
sql("""
drop table if  exists lm_train_area_validate_4_20_7_15_v0  ;
create table lm_train_area_validate_4_20_7_15_v0 as
select 
	lm_user_brand_features_4_20_6_15.user_id,
      lm_user_brand_features_4_20_6_15.brand_id,
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
	actions_by ,

      buy_brand	,
	case
	when buy_brand is not null then 1
	when buy_brand is null then 0
	end as buy_label
from
 lm_user_brand_features_4_20_6_15

left outer join
lm_user_features_4_20_6_15
on 	lm_user_brand_features_4_20_6_15.user_id = lm_user_features_4_20_6_15.user_id

left outer join
lm_brand_features_4_20_6_15
on 	lm_user_brand_features_4_20_6_15.brand_id = lm_brand_features_4_20_6_15.brand_id		


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
on 	lm_user_brand_features_4_20_6_15.user_id = buy_label_table_6_16_7_15.user_id and 	lm_user_brand_features_4_20_6_15.brand_id = buy_label_table_6_16_7_15.buy_brand

where n_actions is not null and  actions_by is not null   ; --fliter
""")




# output table :  lm_train_area_validate

#get model after training :¡¡lm_train_area_validate£ßmodel   

#########################################################################################
# select a model 

#a model : 		 lm_train_area_validate£ßmodel  
#input table : 	 lm_predict_area_validate 

##############     predict   base on tabel lm_predict_area_validate  with model       #########
# get table : lm_predict_area_validate_output
#########################################################################################


# input table :  lm_predict_area_validate_output

######### random tree      ################
# input table :  lm_predict_area_validate_output

sql("""
insert into table evaluation 
select round((hits/pnums),4) precision , round((hits/vnums),4) recall , round((2*hits/(pnums+vnums)),4) F1 , 
        hits , pnums , vnums , getdate() eval_time
from (
    select sum(count_hits(p.brand,v.brand)) hits,
           sum(regexp_count(p.brand,',')+1) pnums,
           sum(regexp_count(v.brand,',')+1) vnums
    from(
	select 
    		user_id, 
    		wm_concat(',',brand_id) as brand -- connect the result
	from(
   		 select
	 		user_id ,
   			brand_id 
  		from  lm_train_area_validate_4_20_7_15_v0_rf_model_1000trees_output     ---------  set table here 
		where probability >= 0.0267   --set thresold value
      	) t
  	group by user_id
	) p
  full outer join lm_validate_set v on p.user_id = v.user_id  
)a;
""")






