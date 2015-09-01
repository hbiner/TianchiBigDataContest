
###
sql("""
create table lmn_brand_date_f_4_11_6_15_extend as
select 
brand_id  ,		 
click_days 	bclick_days,     
buy_days		bbuy_days,
collect_days	bcollect_days,
basket_days	bbasket_days,
sum_days		bsum_days,
dist_days  	bdist_days,

click_days/(dist_days+0.000001) 	bclick_dration,
buy_days/(dist_days+0.000001)   	bbuy_dration,
collect_days/(dist_days+0.000001)   	bcollect_dration,
basket_days/(dist_days+0.000001)   	bbasket_dration,

dist_days/(sum_days+0.000001)      bdist_sum_ration
from  lmn_brand_date_f_4_11_6_15
""")
####

###
sql("""
drop table if exists   lmn_brand_date_f_5_11_7_15_extend;
create table lmn_brand_date_f_5_11_7_15_extend as
select 
brand_id  ,		 
click_days 	bclick_days,     
buy_days		bbuy_days,
collect_days	bcollect_days,
basket_days	bbasket_days,
sum_days		bsum_days,
dist_days  	bdist_days,

click_days/(dist_days+0.000001) 	bclick_dration,
buy_days/(dist_days+0.000001)   	bbuy_dration,
collect_days/(dist_days+0.000001)   	bcollect_dration,
basket_days/(dist_days+0.000001)   	bbasket_dration,

dist_days/(sum_days+0.000001)      bdist_sum_ration
from  lmn_brand_date_f_5_11_7_15
""")
####
###
sql("""
drop table if exists  lmn_brand_date_f_6_11_8_15_extend;
create table lmn_brand_date_f_6_11_8_15_extend as
select 
brand_id  ,		 
click_days 	bclick_days,     
buy_days		bbuy_days,
collect_days	bcollect_days,
basket_days	bbasket_days,
sum_days		bsum_days,
dist_days  	bdist_days,

click_days/(dist_days+0.000001) 	bclick_dration,
buy_days/(dist_days+0.000001)   	bbuy_dration,
collect_days/(dist_days+0.000001)   	bcollect_dration,
basket_days/(dist_days+0.000001)   	bbasket_dration,

dist_days/(sum_days+0.000001)      bdist_sum_ration
from  lmn_brand_date_f_6_11_8_15
""")
####


####
sql("""
create table lmn_predict_area_validate_6_11_8_15_u_b
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
	
	last_datetime

from  lmn_predict_area_validate_6_11_8_15  a

left outer join 
lmn_user_date_f_6_11_8_15_extend b
on a.user_id = b.user_id

left outer join
lmn_brand_date_f_6_11_8_15_extend c
on a.brand_id = c.brand_id ;
""")
