


###
sql("""
drop table if exists lmn_user_date_f_5_11_7_15_extend;
create table lmn_user_date_f_5_11_7_15_extend as
select 
user_id  ,		 
click_days 	uclick_days,     
buy_days		ubuy_days,
collect_days	ucollect_days,
basket_days	ubasket_days,
sum_days		usum_days,
dist_days  	udist_days,

click_days/(dist_days+0.000001) 	uclick_dration,
buy_days/(dist_days+0.000001)    	ubuy_dration,
collect_days/(dist_days+0.000001)    	ucollect_dration,
basket_days/(dist_days+0.000001)    	ubasket_dration,

dist_days/(sum_days+0.000001)       udist_sum_ration
from  lmn_user_date_f_5_11_7_15
""")
####
###
sql("""
drop table if exists  lmn_user_date_f_6_11_8_15_extend;
create table lmn_user_date_f_6_11_8_15_extend as
select 
user_id  ,		 
click_days 	uclick_days,     
buy_days		ubuy_days,
collect_days	ucollect_days,
basket_days	ubasket_days,
sum_days		usum_days,
dist_days  	udist_days,

click_days/(dist_days+0.000001) 	uclick_dration,
buy_days/(dist_days+0.000001)    	ubuy_dration,
collect_days/(dist_days+0.000001)    	ucollect_dration,
basket_days/(dist_days+0.000001)    	ubasket_dration,

dist_days/(sum_days+0.000001)       udist_sum_ration
from  lmn_user_date_f_6_11_8_15
""")
####

