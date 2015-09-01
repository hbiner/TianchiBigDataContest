
sql("""
create table lm_train_area_validate_4_20_7_15_v0_label1  as
select * 
from  lm_train_area_validate_4_20_7_15_v0 
where  buy_label =1 ;

create table lm_train_area_validate_4_20_7_15_v0_label0  as
select * 
from  lm_train_area_validate_4_20_7_15_v0 
where  buy_label =0 ;
""")


table_label1 = Table("lm_train_area_validate_4_20_7_15_v0_label1") ;
Size = table_label1.getRecordCount();
print Size ;
sampleSize  =  10 * Size ;
print sampleSize;
 DataProc.Sample.randomSample( "lm_train_area_validate_4_20_7_15_v0_label0",int(sampleSize) ,
			"lm_train_area_validate_4_20_7_15_v0_label0_sample") ;



sql("""
drop table if exists  lm_train_area_validate_lab01_union ;
create table  lm_train_area_validate_lab01_union   as
select * 
from(
select * from  lm_train_area_validate_4_20_7_15_v0_label1
union all
select * from  lm_train_area_validate_4_20_7_15_v0_label0_sample 
)t ;
""")




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
  		from  lm_train_area_validate_lab01_union_rf_model_300trees_output_2530000 ---------  set table here
      	) t
  	group by user_id
	) p
  full outer join lm_validate_set v on p.user_id = v.user_id  
)a;
""")




