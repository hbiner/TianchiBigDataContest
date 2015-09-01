###   union  all train table ####

sql("""
drop table if exists  lm_train_area_validate_union ;
create table  lm_train_area_validate_union   as
select * 
from(
select * from lm_train_area_validate_4_20_7_15_v0
union all
select * from   lm_train_area_validate_partition_4_13_7_08
)t ;

""")





######### random tree      #################
###   calculate the validation score ##############

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
  		from   lm_train_area_validate_4_20_7_15_v0_rf_model_300trees_504_output  ---------  set table here 
		where probability >= 0.029   --set thresold value
      	) t
  	group by user_id
	) p
  full outer join lm_validate_set v on p.user_id = v.user_id  
)a;
""")



