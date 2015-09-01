sql("""
drop table if exists lmn_ub_df_6_11_8_15 ;
create table lmn_ub_df_6_11_8_15
(
	user_id	string,
	brand_id	string,

	ub_click1	double,
	ub_click3	double,
	ub_click5	double,
	ub_click7	double,
	ub_click10		double,
	ub_click15		double,
	ub_click25		double,
	ub_click26 	double,

	ub_buy1	double,
	ub_buy3	double,
	ub_buy5	double,	
	ub_buy7	double,
	ub_buy10	double,
	ub_buy15	double,
	ub_buy25	double,
	ub_buy26	double,

	ub_collect1	double,
	ub_collect3	double,
	ub_collect5	double,
	ub_collect7	double,
	ub_collect10	double,
	ub_collect15	double,
	ub_collect25	double,
	ub_collect26	double,

	ub_basket1		double,
	ub_basket3		double,
	ub_basket5		double,
	ub_basket7		double,
	ub_basket10	double,
	ub_basket15	double,
	ub_basket25 	double,
	ub_basket26 	double,

	ub_click_days		double,
	ub_buy_days		double,
	ub_collect_days		double,
	ub_basket_days		double,
	ub_sum_days		double,
	action_day_dist		double

)
""")

#############

sql("""
create table lmn_user_date_f_4_11_6_15
(
user_id  		   	string ,
click_days    	double,
buy_days		double,
collect_days	double ,
basket_days	double ,
sum_days		double,
dist_days  	double
)
""")
sql("""
create table lmn_brand_date_f_4_11_6_15
(
brand_id  		   	string ,
click_days    	double,
buy_days		double,
collect_days	double ,
basket_days	double ,
sum_days		double,
dist_days  	double
)
""")
sql("""
create table lmn_user_date_f_5_11_7_15
(
user_id  		   	string ,
click_days    	double,
buy_days		double,
collect_days	double ,
basket_days	double ,
sum_days		double,
dist_days  	double
)
""")
sql("""
create table lmn_brand_date_f_5_11_7_15
(
brand_id  		   	string ,
click_days    	double,
buy_days		double,
collect_days	double ,
basket_days	double ,
sum_days		double,
dist_days  	double
)
""")
###
sql("""
create table lmn_user_date_f_6_11_8_15
(
user_id  		   string ,
click_days    	double,
buy_days		double,
collect_days	double ,
basket_days	double ,
sum_days		double,
dist_days  	double
)
""")
###
sql("""
create table lmn_brand_date_f_6_11_8_15
(
brand_id  		   	string ,
click_days    	double,
buy_days		double,
collect_days	double ,
basket_days	double ,
sum_days		double,
dist_days  	double
)
""")















