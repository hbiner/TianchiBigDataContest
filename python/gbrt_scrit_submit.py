


####  prepare train
train_table = "lmn_train_area_submit_5_11_8_15_lab01_union_xx";
predict_table= "lmn_predict_area_submit_6_11_8_15_xx";
columArray =[2,3,4,5,6,7,8,9,10,
11,12,13,14,15,16,17,18,19,20,
21,22,23,24,25,26,27,28,29,30,
31,32,33,34,35,36,37,38,39,40,
41,42,43,44,45,46,47,48,49,50,
51,52,53,54,55,56,57,58,59,60,
61,62,63,64,65,66,75,76,77,78,79,80,
81,82,83,84,85,86,87,88,89,90,
91,92,93,94,95,103,104,105,106,107,108,109,110,
111,112,113,114,115,116,117,118,119,120,
121,122,123,124,125,126,127,128,129,130,
131,132,133,134,135,136,137,138,139,140,
141,142,143,144,145,146,147,148,149,150,
151,152,153,154,155,156,157,158,159,160,
161,162,163,164,165,166,167,168] ;
labelColName = "buy_label";

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
				treeDepth=10, treesNum=230 ,learningRate=0.05 )


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
if Table.exists("lm_train_area_submit_output_2600000"):
 Table.drop("lm_train_area_submit_output_2600000");

DataProc.topn(train_table+'_lab01_union_sm_gbrt_model_output',
		    2577777,
		    "lm_train_area_submit_output_2600000",
		   ["y_var"],
		    ["-"] )

##

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
  from    lm_train_area_submit_output_2600000   ---
) a 
group by user_id;  
""")



