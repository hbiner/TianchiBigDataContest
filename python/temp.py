sql("""
create table lmn_train_area_submit_5_11_8_15_lab01_union_u_b
as select
	a.user_id,
     a.brand_id,
	click1	 ,
	click3	 ,
	click5	 ,
	click7	 ,
	click10	,
	click15	 ,
	click25	 ,

	buy1	 ,
	buy3	 ,
	buy5	 ,
	buy7	 ,
	buy10	,
	buy15	 ,
	buy25	 ,

	collect1	 ,
	collect3	 ,
	collect5	 ,
	collect7	 ,
	collect10	,
	collect15	 ,
	collect25	 ,

	basket1	 ,
	basket3	 ,
	basket5	 ,
	basket7	 ,
	basket10	,
	basket15	 ,
	basket25	,
      
      click_this_ratio	,
	buy_this_ratio	,
      collect_this_ratio	 ,
      basket_this_ratio	,
	action_this		,

      -- brand features
 	n_clicked_ratio,
	n_bought_ratio,
	n_collected_ratio,	
	n_basketed_ratio,	
	actions_by ,

	clicked_dist_ratio,
	bought_dist_ratio,
	collected_dist_ratio,
	basketed_dist_ratio ,
      users_dist,
 
	--user features 
	n_click_ratio,
	n_buy_ratio,
	n_collect_ratio ,
	n_basket_ratio,
	actions_sum ,

	click_dist_ratio,
	buy_dist_ratio,
	collect_dist_ratio,
	basket_dist_ratio,
	brand_dist,

    --user_date features
	uclick_days,     
	ubuy_days,
	ucollect_days,
	ubasket_days,
	usum_days,
	udist_days,

	uclick_dration,
    	ubuy_dration,
   	ucollect_dration,
  	ubasket_dration,
      udist_sum_ration,

---brand date features 
	bclick_days,     
	bbuy_days,
	bcollect_days,
	bbasket_days,
	bsum_days,
  	bdist_days,

	bclick_dration,
 	bbuy_dration,
 	bcollect_dration,
  	bbasket_dration,
     bdist_sum_ration,
	
	last_datetime,
	buy_label

from lmn_train_area_submit_5_11_8_15_lab01_union  a

left outer join 
lmn_user_date_f_5_11_7_15_extend b
on a.user_id = b.user_id

left outer join
lmn_brand_date_f_5_11_7_15_extend c
on a.brand_id = c.brand_id ;
""")



 








##
train_table = "lmn_train_area_validate_4_11_7_15_lab01_union8";
predict_table= "lmn_predict_area_validate_5_11_7_15";
columArray =[2,3,4,5,6,7,8,9,10,
11,12,13,14,15,16,17,18,19,20,
21,22,23,24,25,26,27,28,29,30,
31,32,33,34,35,36,37,38,39,40,
41,42,43,44,45,46,47,48,49,50,
51,52,53,54] ;
labelColName = "buy_label";


## ==> sm
DataConvert.tableToSparseMatrix( 
 train_table ,
 train_table+"_sm" ,
 selectedColIndex = columArray )

##  train
gbrt_model_12 =Regression.GradBoostRegTree.trainSparse(
				 train_table+"_sm",
				train_table , labelColName,
				train_table+"_sm_gbrt_model",
				treeDepth=8, treesNum=100 ,learningRate=0.1 )
###  predict
## ==> sm
DataConvert.tableToSparseMatrix( 
 predict_table ,
 predict_table+"_sm" ,
 selectedColIndex = columArray )
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
drop table if exists t_tmall_add_user_brand_predict_dh ; --
create table t_tmall_add_user_brand_predict_dh as  --
select --  first
    user_id, 
    wm_concat(',',brand_id) as brand -- connect the result
from
   ( select
	user_id ,
   	brand_id 
  from  lmn_train_area_submit_output_2600000   --
 
) a 
group by user_id;  
""")



