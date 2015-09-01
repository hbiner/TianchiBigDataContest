
sql("""
drop table if exists  lmn_train_area_validate_4_11_7_15_lab1 ;
create table lmn_train_area_validate_4_11_7_15_lab1  as
select * 
from  lmn_train_area_validate_4_11_7_15
where  buy_label =1 ;

drop table if exists  lmn_train_area_validate_4_11_7_15_lab0;
create table lmn_train_area_validate_4_11_7_15_lab0  as
select * 
from lmn_train_area_validate_4_11_7_15
where  buy_label =0 ;
""")

## 1:10
table_label1 = Table("lmn_train_area_validate_4_11_7_15_lab1") ;
Size = table_label1.getRecordCount();
print Size ;
sampleSize10  =  10 * Size ;
print sampleSize10;
 DataProc.Sample.randomSample( "lmn_train_area_validate_4_11_7_15_lab0",int(sampleSize10) ,
			"lmn_train_area_validate_4_11_7_15_label0_sample10") ;
# 1:12
sampleSize12  =  12 * Size ;
print sampleSize12;
 DataProc.Sample.randomSample( "lmn_train_area_validate_4_11_7_15_lab0",int(sampleSize12) ,
			"lmn_train_area_validate_4_11_7_15_label0_sample12") ;
sql("""
drop table if exists  lmn_train_area_validate_4_11_7_15_lab01_union12 ;
create table  lmn_train_area_validate_4_11_7_15_lab01_union12   as
select * 
from(
select * from  lmn_train_area_validate_4_11_7_15_lab1
union all
select * from  lmn_train_area_validate_4_11_7_15_label0_sample12
)t ;
""")
#1:8
sampleSize8  =  8 * Size ;
print sampleSize8
 DataProc.Sample.randomSample( "lmn_train_area_validate_4_11_7_15_lab0",int(sampleSize8) ,
			"lmn_train_area_validate_4_11_7_15_label0_sample8") ;




//  training

##  gbrt 
sql(""" drop table if exists  lmn_train_area_validate_4_11_7_15_lab01_union_sm_gbrt_model_buylabel_output ; """)
DataProc.appendColumns (['lmn_predict_area_validate_5_11_7_15',
				'lmn_train_area_validate_4_11_7_15_lab01_union_sm_gbrt_model_labelval_yvar'],
				'lmn_train_area_validate_4_11_7_15_lab01_union_sm_gbrt_model_labelval_output')

#  get top 2547331
sql("""drop table if exists lm_train_area_validate_output_2547331;""")
 DataProc.topn( "lmn_train_area_validate_4_11_7_15_lab01_union_sm_gbrt_model_labelval_output",
		    2547331,
		    "lm_train_area_validate_output_2547331",
		   ["y_var"],
		    ["-"] );



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
  		from  lm_train_area_validate_output_2547331 -------  set table here

      	) t
  	group by user_id
	) p
  full outer join validate_set v on p.user_id = v.user_id  
)a;
""")

##########################################################









