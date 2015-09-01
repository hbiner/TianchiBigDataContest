##  validate ##
sql(""" drop table if exists lm_train_area_validate_4_20_7_15_v0_gbrt_model_output ; """)
DataProc.appendColumns (['lm_predict_area_validate_5_20_7_15_v0',
				'lm_train_area_validate_lab01_union_sm_gbrt_model_yvar'],
				'lm_train_area_validate_4_20_7_15_v0_gbrt_model_output')



 DataProc.topn( "lm_train_area_validate_4_20_7_15_v0_gbrt_model_output",
		    2530000,
		    "lm_train_area_validate_4_20_7_15_v0_gbrt_model_output_2530000",
		   ["y_var"],
		    ["-"] );


############## validate calculate   ######################

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
  		from lm_train_area_validate_4_20_7_15_v0_gbrt_model_output_2530000    ---------  set table here 
      	) t
  	group by user_id
	) p
  full outer join lm_validate_set v on p.user_id = v.user_id  
)a;
""")




############################################################################################




##  submit ##
sql(""" drop table if exists  lm_train_area_submit_5_20_8_15_sm_gbrt_model_output ; """)
DataProc.appendColumns ([ 'lm_predict_area_submit_6_20_8_15',
				'lm_train_area_submit_5_20_8_15_sm_gbrt_model_80trees_5d_yvar'],
				'lm_train_area_submit_5_20_8_15_sm_gbrt_model_output')

### create  t_tmall_add_user_brand_predict_dh  #####
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
  from lm_train_area_submit_5_20_8_15_sm_gbrt_model_output
	where y_var >= 0.0199     -- set the threshold value -- 
) a
  
group by user_id;  
""")
###########





