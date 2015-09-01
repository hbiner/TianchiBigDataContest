sql("""
drop table if exists lmn_train_area_submit_5_11_8_15_lab01_union_u_b;
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
	click26	 ,

	buy1	 ,
	buy3	 ,
	buy5	 ,
	buy7	 ,
	buy10	,
	buy15	 ,
	buy25	 ,
	buy26 ,

	collect1	 ,
	collect3	 ,
	collect5	 ,
	collect7	 ,
	collect10	,
	collect15	 ,
	collect25	 ,
	collect26	 ,

	basket1	 ,
	basket3	 ,
	basket5	 ,
	basket7	 ,
	basket10	,
	basket15	 ,
	basket25	,
	basket26	,
      
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

####





sql("""
drop table if exists  lmn_predict_area_submit_6_11_8_15_u_b;
create table lmn_predict_area_submit_6_11_8_15_u_b
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
	click26	 ,

	buy1	 ,
	buy3	 ,
	buy5	 ,
	buy7	 ,
	buy10	,
	buy15	 ,
	buy25	 ,
	buy26 ,

	collect1	 ,
	collect3	 ,
	collect5	 ,
	collect7	 ,
	collect10	,
	collect15	 ,
	collect25	 ,
	collect26	 ,

	basket1	 ,
	basket3	 ,
	basket5	 ,
	basket7	 ,
	basket10	,
	basket15	 ,
	basket25	,
	basket26	,
      
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
	
	last_datetime

from  lmn_predict_area_submit_6_11_8_15  a

left outer join 
lmn_user_date_f_6_11_8_15_extend b
on a.user_id = b.user_id

left outer join
lmn_brand_date_f_6_11_8_15_extend c
on a.brand_id = c.brand_id ;
""")
