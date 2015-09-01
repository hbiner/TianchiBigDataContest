###   union  all train table ####

sql("""
drop table if exists  lm_train_area_submit_union ;
create table  lm_train_area_submit_union   as
select * 
from(
select * from  lm_train_area_submit_5_20_8_15
union all
select * from  lm_train_area_submit_partition_5_13_8_08
union all
select * from  lm_train_area_submit_partition_5_06_8_01
union all
select * from  lm_train_area_submit_partition_4_29_7_24
union all
select * from  lm_train_area_submit_partition_4_22_7_17
)t ;

""")



## output 
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
  from  lm_train_area_submit_union_rf_model_1000trees_1004_output
	where probability >= 0.0254   -- set the threshold value -- 
) a
  
group by user_id;  
""")









