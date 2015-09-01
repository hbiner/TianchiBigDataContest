
## 1:8
table_label1 = Table("lmn_train_area_validate_4_11_7_15_lab1") ;
Size = table_label1.getRecordCount();
print Size ;
sampleSize  =  8 * Size ;
print sampleSize;
DataProc.Sample.randomSample( "lmn_train_area_validate_4_11_7_15_lab0",int(sampleSize) ,
			"lmn_train_area_validate_4_11_7_15_label0_sample8") ;
##
## 1:10
sql("""
drop table if exists  lmn_train_area_validate_4_11_7_15_lab01_union8 ;
create table  lmn_train_area_validate_4_11_7_15_lab01_union8   as
select * 
from(
select * from  lmn_train_area_validate_4_11_7_15_lab1
union all
select * from  lmn_train_area_validate_4_11_7_15_label0_sample8
)t ;
""")
###

####  prepare train
train_table = "lmn_train_area_validate_4_11_7_15_lab01_union_x";
predict_table= "lmn_predict_area_validate_5_11_7_15_x";
columArray =[2,3,4,5,6,7,8,9,10,
11,12,13,14,15,16,17,18,19,20,
21,22,30,
31,32,33,34,35,36,37,38,39,40,
41,42,43,44,45,46,47,48,49,50,
51,52,53,54,55,56,57,58,59,67,68,69,70,
71,72,73,74,75,76,77,78,79,80,
81,82,83,84,85,86,87,95,96,97,98,99,100,
101,102,103,104,105,106,107,108,109,110,
111,112,113,114,115,116,117,118,119,120,
121,122,123,124,125,126,127,128,129,130,
131,132,133,134,135,136,137,138,139,140,
141,142,143,144,145,146,147,148,149,150,
151,152,153,154,155,156,157,158,159,160] ;
labelColName = "buy_label";
learn_rate = 0.1;
tree_depth = 10;
trees_num = 50;

## ==> sm
if Table.exists( train_table+"_sm"):
 Table.drop( train_table+"_sm");
DataConvert.tableToSparseMatrix( 
 train_table ,
 train_table+"_sm" ,
 selectedColIndex = columArray )

##  train
if Table.exists(train_table+"_sm_gbrt_model"):
 Table.drop( train_table+"_sm_gbrt_model");
gbrt_model_12 =Regression.GradBoostRegTree.trainSparse(
				 train_table+"_sm",
				train_table , labelColName,
				train_table+"_sm_gbrt_model",
				treeDepth=tree_depth, treesNum=trees_num ,learningRate=learn_rate )


###  predict
## ==> sm
if Table.exists( predict_table+"_sm"):
 Table.drop( predict_table+"_sm");
DataConvert.tableToSparseMatrix( 
 predict_table ,
 predict_table+"_sm" ,
 selectedColIndex = columArray )
## prdicting 
if Table.exists(train_table+"_sm_gbrt_model_yvar"):
 Table.drop(train_table+"_sm_gbrt_model_yvar");
Regression.GradBoostRegTree.predictSparse(predict_table+"_sm",
					gbrt_model_12,
					train_table+"_sm_gbrt_model_yvar",
				      )
###
##  gbrt 
if Table.exists(train_table+"_lab01_union_sm_gbrt_model_output"):
 Table.drop(train_table+"_lab01_union_sm_gbrt_model_output");
DataProc.appendColumns ([predict_table,
				train_table+"_sm_gbrt_model_yvar"],
				train_table +'_lab01_union_sm_gbrt_model_output')


#  get top  N
if Table.exists("lm_train_area_validate_output_2547331"):
 Table.drop("lm_train_area_validate_output_2547331");

DataProc.topn(train_table+'_lab01_union_sm_gbrt_model_output',
		    2547331,
		    "lm_train_area_validate_output_2547331",
		   ["y_var"],
		    ["-"] )

##
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
  		from  lm_train_area_validate_output_2547331  -------  set table here

      	) t
  	group by user_id
	) p
  full outer join validate_set v on p.user_id = v.user_id  
)a;
""")


