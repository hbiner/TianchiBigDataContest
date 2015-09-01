
sql("""
create table lmn_train_area_validate_4_11_7_15_lab1  as
select * 
from  lmn_train_area_validate_4_11_7_15
where  buy_label =1 ;

create table lmn_train_area_validate_4_11_7_15_lab0  as
select * 
from lmn_train_area_validate_4_11_7_15
where  buy_label =0 ;
""")


table_label1 = Table("lmn_train_area_validate_4_11_7_15_lab1") ;
Size = table_label1.getRecordCount();
print Size ;
sampleSize  =  10 * Size ;
print sampleSize;
 DataProc.Sample.randomSample( "lmn_train_area_validate_4_11_7_15_lab0",int(sampleSize) ,
			"lmn_train_area_validate_4_11_7_15_label0_sample") ;


sql("""
drop table if exists  lmn_train_area_validate_4_11_7_15_lab01_union ;
create table  lmn_train_area_validate_4_11_7_15_lab01_union   as
select * 
from(
select * from  lmn_train_area_validate_4_11_7_15_lab1
union all
select * from  lmn_train_area_validate_4_11_7_15_label0_sample
)t ;
""")


//  training
sql(""" drop table if exists lm_train_area_submit_5_20_8_15_sm_gbrt_model_50t_8d_output ; """)
DataProc.appendColumns (['lm_predict_area_submit_6_20_8_15',
				'lm_train_area_submit_5_20_8_15_sm_gbrt_model_50t_8d_yvar'],
				'lm_train_area_submit_5_20_8_15_sm_gbrt_model_50t_8d_output')
##  gbrt 
sql(""" drop table if exists lmn_train_area_validate_gbrt_model_output ; """)
DataProc.appendColumns (['lmn_predict_area_validate_5_11_7_15',
				'lmn_train_area_validate_4_11_7_15_sm_gbrt_model_yvar'],
				'lmn_train_area_validate_gbrt_model_output')




#  get top 2530000
sql("""drop table if exists lm_train_area_validate_output_2547331;""")
 DataProc.topn( "lmn_train_area_validate_4_11_7_15_addclass_rf_model_300t_output",
		    2547331,
		    "lm_train_area_validate_output_2547331",
		   ["probability"],
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
  full outer join lm_validate_set v on p.user_id = v.user_id  
)a;
""")

##########################################################






