# train area
sql("""
create table  lmn_u_or_b_features_5_11_7_15_temp
(
	brand_id	string,
	user_id     string,
	clicked	double,
	bought	double,
	collected	double,
	basketed	double,
	actions	double
)
""")


sql("""
drop table if exists lmn_user_brand_features_5_11_7_15_t;
create table lmn_user_brand_features_5_11_7_15_t
(
	user_id	string,
	brand_id	string,
	click1	double ,
	click3	double ,
	click5	 double,
	click7	 double,
	click10	double,
	click15	 double,
	click25	 double,
	click26	 double,

	buy1	 double,
	buy3	 double,
	buy5	 double,
	buy7	 double,
	buy10	 double,
	buy15	 double,
	buy25	 double,
	buy26	 double,

	collect1	double ,
	collect3	 double,
	collect5	 double,
	collect7	 double,
	collect10	double,
	collect15	 double,
	collect25	 double,
	collect26	 double,

	basket1		 double,
	basket3		 double,
	basket5		 double,
	basket7		 double,
	basket10	     	double,
	basket15		 double,
	basket25		double,
	basket26		double,

     click_this		double,
	buy_this    	double,
	collect_this 	 double,
	basket_this	  	double,
	action_this		double,
	last_datetime	string
)
""")

##############################
###   predict area 

sql("""
create table  lmn_u_or_b_features_6_11_8_15_temp
(
	brand_id	string,
	user_id     string,
	clicked	double,
	bought	double,
	collected	double,
	basketed	double,
	actions	double
)
""")
# create brandFeature table
sql("""
drop table if exists lmn_user_brand_features_6_11_8_15_t;
create table lmn_user_brand_features_6_11_8_15_t
(
	user_id	string,
	brand_id	string,
	click1	double ,
	click3	double ,
	click5	 double,
	click7	 double,
	click10	double,
	click15	 double,
	click25	 double,
	click26	 double,

	buy1	 double,
	buy3	 double,
	buy5	 double,
	buy7	 double,
	buy10	 double,
	buy15	 double,
	buy25	 double,
	buy26	 double,

	collect1	double ,
	collect3	 double,
	collect5	 double,
	collect7	 double,
	collect10	double,
	collect15	 double,
	collect25	 double,
	collect26	 double,

	basket1		 double,
	basket3		 double,
	basket5		 double,
	basket7		 double,
	basket10	     	double,
	basket15		 double,
	basket25		double,
	basket26		double,

     click_this		double,
	buy_this    	double,
	collect_this 	 double,
	basket_this	  	double,
	action_this		double,
	last_datetime	string
)
""")

###
// new
###


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








