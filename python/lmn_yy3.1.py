######
sql("""
create table lmn_train_area_submit_5_11_8_15_lab01_union_xx
as select
	a.user_id,
     a.brand_id,
	click1  ,
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

      click_this_ratio*action_this click_this	,
	buy_this_ratio*action_this	buy_this	,
      collect_this_ratio*action_this	collect_this	 ,
      basket_this_ratio*action_this		basket_this,
	action_this		,

	--ub date features
ub_click1	 ,
	ub_click3	 ,
	ub_click5	 ,
	ub_click7	 ,
	ub_click10		 ,
	ub_click15		 ,
	ub_click25		 ,
	ub_click26		 ,

	ub_buy1	 ,
	ub_buy3	 ,
	ub_buy5	 ,	
	ub_buy7	 ,
	ub_buy10	 ,
	ub_buy15	 ,
	ub_buy25	 ,
	ub_buy26	 ,

	ub_collect1	 ,
	ub_collect3	 ,
	ub_collect5	 ,
	ub_collect7	 ,
	ub_collect10	 ,
	ub_collect15	 ,
	ub_collect25	 ,
	ub_collect26	 ,

	ub_basket1		 ,
	ub_basket3		 ,
	ub_basket5		 ,
	ub_basket7		 ,
	ub_basket10	 ,
	ub_basket15	 ,
	ub_basket25 	 ,
	ub_basket26 	 ,

     --ub_ub
     click1/(ub_click1+0.000001)	 	click_avgpday1	 ,
	click3/(ub_click3+0.000001)		click_avgpday3 ,
	click5/(ub_click5+0.000001)		click_avgpday5 ,
	click7/(ub_click7+0.000001)		click_avgpday7 ,
	click10/(ub_click10	+0.000001)	 click_avgpday10	 ,
	click15/(ub_click15	+0.000001)	 click_avgpday15	 ,
	click25/(ub_click25+0.000001)	click_avgpday25	 ,

	buy1/(ub_buy1+0.000001)		buy_avgpday1	 ,
	buy3/(ub_buy3+0.000001)		buy_avgpday3 ,
	buy5/(ub_buy5+0.000001)		 buy_avgpday5,	
	buy7/(ub_buy7+0.000001)		 buy_avgpday7,
	buy10/(ub_buy10+0.000001)		 buy_avgpday10,
	buy15/(ub_buy15+0.000001)		 buy_avgpday15,
	buy25/(ub_buy25+0.000001)		 buy_avgpday25,

	collect1/(ub_collect1+0.000001)		 collect_avgpday1	 ,
	collect3/(ub_collect3+0.000001)		 collect_avgpday3,
	collect5/(ub_collect5+0.000001)		 collect_avgpday5,
	collect7/(ub_collect7+0.000001)		 collect_avgpday7,
	collect10/(ub_collect10+0.000001)	     collect_avgpday10 ,
	collect15/(ub_collect15+0.000001)		 collect_avgpday15,
	collect25/(ub_collect25+0.000001)		 collect_avgpday25,


	basket1/(ub_basket1	+0.000001)		 basket_avgpday1,
	basket3/(ub_basket3	+0.000001)		 basket_avgpday3,
	basket5/(ub_basket5	+0.000001)		 basket_avgpday5,
	basket7/(ub_basket7	+0.000001)		 basket_avgpday7,
	basket10/(ub_basket10+0.000001)		 	 basket_avgpday10,
	basket15/(ub_basket15+0.000001)		 	 basket_avgpday15,
	basket25/(ub_basket25+0.000001)	 	 basket_avgpday25,

	ub_click_days		 ,
	ub_buy_days		 ,
	ub_collect_days		 ,
	ub_basket_days		 ,

	ub_click_days/(ub_sum_days+0.000001)	ub_click_sumd_ratio	 ,
	ub_buy_days/(ub_sum_days+0.000001)		ub_buy_sumd_ratio ,
	ub_collect_days/(ub_sum_days+0.000001)	ub_collect_sumd_ratio	 ,
	ub_basket_days/(ub_sum_days+0.000001)	ub_basket_sumd_ratio	 ,

	ub_click_days/(action_day_dist+0.000001)	ub_click_distd_ratio	 ,
	ub_buy_days/(action_day_dist+0.000001)	ub_buy_distd_ratio	 ,
	ub_collect_days/(action_day_dist+0.000001) ub_collect_distd_ratio ,
	ub_basket_days/(action_day_dist+0.000001)	  ub_basket_distd_ratio	 ,

	ub_sum_days		 ,
	action_day_dist		 ,

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

--brand date features 
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
	
     --  cross features
	--  ub-u
     	ub_click_days/(uclick_days+0.000001)	ubdu_click_dratio	 ,
	ub_buy_days/(ubuy_days+0.000001)		ubdu_buy_dratio		 ,
	ub_collect_days/(ucollect_days+0.000001)	ubdu_collect_dratio		 ,
	ub_basket_days/(ubasket_days+0.000001)	ubdu_basket_dratio		 ,
	ub_sum_days/(usum_days+0.000001)		ubdu_sum_dratio ,
	action_day_dist	/(udist_days+0.000001)	ubdu_day_dist_dratio ,
	--  ub-b
	 click_this_ratio*action_this*(n_clicked_ratio  )	ubmb_click_dratio	 ,
 	buy_this_ratio*action_this*(n_bought_ratio  )	 	ubmb_buy_dratio		 ,
	 collect_this_ratio*action_this*(n_collected_ratio )	ubmb_collect_dratio		 ,
	 basket_this_ratio*action_this*(n_basketed_ratio  )	ubmb_basket_dratio		 ,
	
       
	last_datetime,
    buy_label



from lmn_train_area_submit_5_11_8_15_lab01_union_u_b  a

left outer join 
lmn_ub_df_5_11_7_15 d
on  a.user_id = d.user_id and a.brand_id = d.brand_id ;

""")
###






###
sql("""
create table lmn_predict_area_submit_6_11_8_15_xx
as select
	a.user_id,
     a.brand_id,
	click1  ,
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

      click_this_ratio*action_this click_this	,
	buy_this_ratio*action_this	buy_this	,
      collect_this_ratio*action_this	collect_this	 ,
      basket_this_ratio*action_this		basket_this,
	action_this		,

	--ub date features
ub_click1	 ,
	ub_click3	 ,
	ub_click5	 ,
	ub_click7	 ,
	ub_click10		 ,
	ub_click15		 ,
	ub_click25		 ,
	ub_click26		 ,

	ub_buy1	 ,
	ub_buy3	 ,
	ub_buy5	 ,	
	ub_buy7	 ,
	ub_buy10	 ,
	ub_buy15	 ,
	ub_buy25	 ,
	ub_buy26	 ,

	ub_collect1	 ,
	ub_collect3	 ,
	ub_collect5	 ,
	ub_collect7	 ,
	ub_collect10	 ,
	ub_collect15	 ,
	ub_collect25	 ,
	ub_collect26	 ,

	ub_basket1		 ,
	ub_basket3		 ,
	ub_basket5		 ,
	ub_basket7		 ,
	ub_basket10	 ,
	ub_basket15	 ,
	ub_basket25 	 ,
	ub_basket26 	 ,

     --ub_ub
     click1/(ub_click1+0.000001)	 	click_avgpday1	 ,
	click3/(ub_click3+0.000001)		click_avgpday3 ,
	click5/(ub_click5+0.000001)		click_avgpday5 ,
	click7/(ub_click7+0.000001)		click_avgpday7 ,
	click10/(ub_click10	+0.000001)	 click_avgpday10	 ,
	click15/(ub_click15	+0.000001)	 click_avgpday15	 ,
	click25/(ub_click25+0.000001)	click_avgpday25	 ,

	buy1/(ub_buy1+0.000001)		buy_avgpday1	 ,
	buy3/(ub_buy3+0.000001)		buy_avgpday3 ,
	buy5/(ub_buy5+0.000001)		 buy_avgpday5,	
	buy7/(ub_buy7+0.000001)		 buy_avgpday7,
	buy10/(ub_buy10+0.000001)		 buy_avgpday10,
	buy15/(ub_buy15+0.000001)		 buy_avgpday15,
	buy25/(ub_buy25+0.000001)		 buy_avgpday25,

	collect1/(ub_collect1+0.000001)		 collect_avgpday1	 ,
	collect3/(ub_collect3+0.000001)		 collect_avgpday3,
	collect5/(ub_collect5+0.000001)		 collect_avgpday5,
	collect7/(ub_collect7+0.000001)		 collect_avgpday7,
	collect10/(ub_collect10+0.000001)	     collect_avgpday10 ,
	collect15/(ub_collect15+0.000001)		 collect_avgpday15,
	collect25/(ub_collect25+0.000001)		 collect_avgpday25,


	basket1/(ub_basket1	+0.000001)		 basket_avgpday1,
	basket3/(ub_basket3	+0.000001)		 basket_avgpday3,
	basket5/(ub_basket5	+0.000001)		 basket_avgpday5,
	basket7/(ub_basket7	+0.000001)		 basket_avgpday7,
	basket10/(ub_basket10+0.000001)		 	 basket_avgpday10,
	basket15/(ub_basket15+0.000001)		 	 basket_avgpday15,
	basket25/(ub_basket25+0.000001)	 	 basket_avgpday25,

	ub_click_days		 ,
	ub_buy_days		 ,
	ub_collect_days		 ,
	ub_basket_days		 ,

	ub_click_days/(ub_sum_days+0.000001)	ub_click_sumd_ratio	 ,
	ub_buy_days/(ub_sum_days+0.000001)		ub_buy_sumd_ratio ,
	ub_collect_days/(ub_sum_days+0.000001)	ub_collect_sumd_ratio	 ,
	ub_basket_days/(ub_sum_days+0.000001)	ub_basket_sumd_ratio	 ,

	ub_click_days/(action_day_dist+0.000001)	ub_click_distd_ratio	 ,
	ub_buy_days/(action_day_dist+0.000001)	ub_buy_distd_ratio	 ,
	ub_collect_days/(action_day_dist+0.000001) ub_collect_distd_ratio ,
	ub_basket_days/(action_day_dist+0.000001)	  ub_basket_distd_ratio	 ,

	ub_sum_days		 ,
	action_day_dist		 ,

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

--brand date features 
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
	
     --  cross features
	--  ub-u
     	ub_click_days/(uclick_days+0.000001)	ubdu_click_dratio	 ,
	ub_buy_days/(ubuy_days+0.000001)		ubdu_buy_dratio		 ,
	ub_collect_days/(ucollect_days+0.000001)	ubdu_collect_dratio		 ,
	ub_basket_days/(ubasket_days+0.000001)	ubdu_basket_dratio		 ,
	ub_sum_days/(usum_days+0.000001)		ubdu_sum_dratio ,
	action_day_dist	/(udist_days+0.000001)	ubdu_day_dist_dratio ,
	--  ub-b
	 click_this_ratio*action_this*(n_clicked_ratio  )	ubmb_click_dratio	 ,
 	buy_this_ratio*action_this*(n_bought_ratio  )	 	ubmb_buy_dratio		 ,
	 collect_this_ratio*action_this*(n_collected_ratio )	ubmb_collect_dratio		 ,
	 basket_this_ratio*action_this*(n_basketed_ratio  )	ubmb_basket_dratio		 ,
	
       
	last_datetime
  

from  lmn_predict_area_submit_6_11_8_15_u_b  a

left outer join 
lmn_ub_df_6_11_8_15 d
on  a.user_id = d.user_id and a.brand_id = d.brand_id ;
""")


