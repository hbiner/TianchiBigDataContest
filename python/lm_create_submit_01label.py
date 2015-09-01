
##    proc  table :   lm_train_area_submit_union


sql("""
drop table if exists lm_train_area_submit_union_label1 ;
create table lm_train_area_submit_union_label1  as
select * 
from  lm_train_area_submit_union
where  buy_label =1 ;

create table lm_train_area_submit_union_label0  as
select * 
from  lm_train_area_submit_union
where  buy_label =0 ;
""")



Size =  Table("lm_train_area_submit_union_label1") .getRecordCount();
print Size ;
sampleSize  =  10 * Size ;
print sampleSize;
 DataProc.Sample.randomSample( "lm_train_area_submit_union_label0",int(sampleSize) ,
			"lm_train_area_submit_union_label0_sample") ;



sql("""
drop table if exists  lm_train_area_submit_lab01_union ;
create table  lm_train_area_submit_lab01_union   as
select * 
from(
select * from lm_train_area_submit_union_label1
union all
select * from  lm_train_area_submit_union_label0_sample 
)t ;
""")



///    training            

//get top 2830000
 DataProc.topn( "lm_train_area_submit_lab01_union_rf_model_1000trees_100w_output",
		    2830000,
		    "lm_train_area_submit_lab01_union_rf_model_1000trees_100w_output_2830000",
		   ["probability"],
		    ["-"] );



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
  from lm_train_area_submit_lab01_union_rf_model_1000trees_100w_output_2830000 		 -----
) a 
group by user_id;  
""")




