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
drop table if  exists lm_train_area_validate_4_20_7_15  ;
create table lm_train_area_validate_4_20_7_15 as
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

	uclick1,
	uclick3,
	uclick5,
	uclick7,
	uclick15,
	uclick25,

      ubuy1,
      ubuy3,
      ubuy5,
      ubuy7,
      ubuy15,
      ubuy25,

      ucollect1,
      ucollect3,
	ucollect5,
	ucollect7,
	ucollect15,
	ucollect25,

	ubasket1,
	ubasket3,
	ubasket5,
	ubasket7,
	ubasket15,
	ubasket25 ,

	asked_users ,
   	n_clicked,
 	n_bought,
	n_collected,
	n_basketed,
	actions_by ,

      bclicked1,
	bclicked3,
	bclicked5,
 	bclicked7,
 	bclicked15,
 	bclicked25,

       bbought1,
       bbought3,
       bbought5,
       bbought7,
       bbought15,
       bbought25,

	 bcollected1,
	 bcollected3,
	 bcollected5,
	 bcollected7,
	 bcollected15,
 	 bcollected25,

	bbasketed1,
 	bbasketed3,
	bbasketed5,
 	bbasketed7,
	bbasketed15,
 	bbasketed25,

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

left outer join
lm_user_date_features_4_20_6_15
on 	lm_user_brand_features_4_20_6_15.user_id = lm_user_date_features_4_20_6_15.user_id

left outer join
lm_brand_date_features_4_20_6_15
on 	lm_user_brand_features_4_20_6_15.brand_id = lm_brand_date_features_4_20_6_15.brand_id		



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
  		from  lm_train_area_validate_4_20_7_15_rf_model_c43_20depth_300trees_output     ---------  set table here 
		where probability >= 0.024   --set thresold value
      	) t
  	group by user_id
	) p
  full outer join lm_validate_set v on p.user_id = v.user_id  
)a;
""")




