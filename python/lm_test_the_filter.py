################################
#
#         test the filter


#connect all
sql("""
drop table if  exists lm_predict_area_validate_5_20_7_15_with_filter  ;
create table lm_predict_area_validate_5_20_7_15_with_filter as
select *
from 
lm_predict_area_validate_5_20_7_15
where n_actions  >= 10 or click1 >=1 or click3 >=1  ;

""")





#connect all
sql("""

drop table if  exists lm_train_area_validate_4_20_7_15_with_filter  ;
create table lm_train_area_validate_4_20_7_15_with_filter as
select   *
from lm_train_area_validate_4_20_7_15
where n_actions  >= 10 or click1 >= 1 or click3 >=1   ;

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
  		from  lm_train_area_validate_4_20_7_15_rf_model_300trees_20depth_output     ---------  set table here 
		where probability >= 0.0265  --set thresold value
      	) t
  	group by user_id
	) p
  full outer join lm_validate_set v on p.user_id = v.user_id  
)a;
""")


