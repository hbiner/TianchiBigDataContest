
#connect all
sql("""
drop table if  exists lm_train_area_validate_4_20_7_15  ;
create table lm_train_area_validate_4_20_7_15 as
select 
	lm_train_area_validate_4_20_7_15_v0_1order.user_id,
      lm_train_area_validate_4_20_7_15_v0_1order.brand_id,
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

	p_askbrands,
     	p_click,
	p_buy,
	p_collect,
	p_basket ,
      user_m ,
       u_1order ,
       u_2order ,

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

	n_clicked * n_bought  cro_clickedbought,
	n_clicked * n_collected cro_clickedcollected,
	n_clicked * n_basketed  cro_clickedbasketed,
	n_bought * n_collected	cro_boughtcollected,
	n_bought * n_basketed	cro_boughtbasketed,
	n_collected * n_basketed cro_collectedbasketed,

	p_askedusers,
	p_clicked,
	p_bought,
	p_collected,
	p_basketed ,
  	 brand_m ,
 	 b_1order ,
 	 b_2order ,



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

    
	 buy_label
from
 lm_train_area_validate_4_20_7_15_v0_1order

left outer join
lm_user_date_features_4_20_6_15
on 	lm_train_area_validate_4_20_7_15_v0_1order.user_id = lm_user_date_features_4_20_6_15.user_id

left outer join
lm_brand_date_features_4_20_6_15
on 	lm_train_area_validate_4_20_7_15_v0_1order.brand_id = lm_brand_date_features_4_20_6_15.brand_id		


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




#connect all
sql("""
drop table if  exists lm_predict_area_validate_5_20_7_15 ;
create table lm_predict_area_validate_5_20_7_15 as
select 
	lm_predict_area_validate_5_20_7_15_v0_1order.user_id,
      lm_predict_area_validate_5_20_7_15_v0_1order.brand_id,
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

	p_askbrands,
     	p_click,
	p_buy,
	p_collect,
	p_basket ,
      user_m ,
       u_1order ,
       u_2order ,

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

	n_clicked * n_bought  cro_clickedbought,
	n_clicked * n_collected cro_clickedcollected,
	n_clicked * n_basketed  cro_clickedbasketed,
	n_bought * n_collected	cro_boughtcollected,
	n_bought * n_basketed	cro_boughtbasketed,
	n_collected * n_basketed cro_collectedbasketed,

	p_askedusers,
	p_clicked,
	p_bought,
	p_collected,
	p_basketed ,
  	 brand_m ,
 	 b_1order ,
 	 b_2order ,



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
 	bbasketed25
from
 lm_predict_area_validate_5_20_7_15_v0_1order

left outer join
lm_user_date_features_5_20_7_15
on 	lm_predict_area_validate_5_20_7_15_v0_1order.user_id = lm_user_date_features_5_20_7_15.user_id

left outer join
lm_brand_date_features_5_20_7_15
on 	lm_predict_area_validate_5_20_7_15_v0_1order.brand_id = lm_brand_date_features_5_20_7_15.brand_id		


""")




# output table :  lm_train_area_validate

#get model after training :¡¡lm_train_area_validate£ßmodel   

#########################################################################################








