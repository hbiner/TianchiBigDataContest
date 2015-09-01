# 调节 正负  样例比 
sql("""
create table lmn_train_area_submit_5_11_8_15_lab1  as
select * 
from  lmn_train_area_submit_5_11_8_15
where  buy_label >=1 ;

create table lmn_train_area_submit_5_11_8_15_lab0  as
select * 
from  lmn_train_area_submit_5_11_8_15
where  buy_label =0 ;
""")

table_label1 = Table("lmn_train_area_submit_5_11_8_15_lab1") ;
Size = table_label1.getRecordCount();
print Size ;
sampleSize  =  9 * Size ;
print sampleSize;
 DataProc.Sample.randomSample( "lmn_train_area_submit_5_11_8_15_lab0",int(sampleSize) ,
			"lmn_train_area_submit_5_11_8_15_lab0_sample") ;


sql("""
drop table if exists   lmn_train_area_submit_5_11_8_15_lab01_union ;
create table  lmn_train_area_submit_5_11_8_15_lab01_union   as
select * 
from(
select * from  lmn_train_area_submit_5_11_8_15_lab1
union all
select * from  lmn_train_area_submit_5_11_8_15_lab0_sample
)t ;
""")
####
#  train with table  lmn_train_area_submit_5_11_8_15_lab01_union

##  gbrt
sql(""" drop table if exists lmn_train_area_submit_lab01_union_sm_gbrt_model_output ; """)
DataProc.appendColumns (['lmn_predict_area_submit_6_11_8_15',
				'lmn_train_area_submit_5_11_8_15_lab01_union_sm_gbrt_model2_yvar'],
				'lmn_train_area_submit_lab01_union_sm_gbrt_model_output')



###########################################################

// get top   2835057
sql(""" drop table if exists lmn_train_area_submit_output_2835057 ;""")
 DataProc.topn( "lmn_train_area_submit_5_11_8_15_lab01_union_rf_model_output",
		    2774409,
		    "lmn_train_area_submit_output_2774409",
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
  from  lmn_train_area_submit_output_2774409   --
 
) a 
group by user_id;  
""")





