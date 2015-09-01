
 ###     pre from here with model ##########
# create table of user_brand  feature
sql("""
create table lmn_user_brand_features_4_11_6_15_t
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
 
	buy1	 double,
	buy3	 double,
	buy5	 double,
	buy7	 double,
	buy10	 double,
	buy15	 double,
	buy25	 double,

	collect1	double ,
	collect3	 double,
	collect5	 double,
	collect7	 double,
	collect10	double,
	collect15	 double,
	collect25	 double,

	basket1		 double,
	basket3		 double,
	basket5		 double,
	basket7		 double,
	basket10	     	double,
	basket15		 double,
	basket25		double,

     click_this		double,
	buy_this    	double,
	collect_this 	 double,
	basket_this	  	double,
	action_this		double,
	last_datetime	string
)
""")


#create table of  brand feature
# create brandFeature table
sql("""
create table  lmn_u_or_b_features_4_11_6_15_temp
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

#####################################
 ###     pre from here with model ##########
# create table of user_brand  feature
sql("""
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

	buy1	 double,
	buy3	 double,
	buy5	 double,
	buy7	 double,
	buy10	 double,
	buy15	 double,
	buy25	 double,

	collect1	double ,
	collect3	 double,
	collect5	 double,
	collect7	 double,
	collect10	double,
	collect15	 double,
	collect25	 double,

	basket1		 double,
	basket3		 double,
	basket5		 double,
	basket7		 double,
	basket10	     	double,
	basket15		 double,
	basket25		double,

     click_this		double,
	buy_this    	double,
	collect_this 	 double,
	basket_this	  	double,
	action_this		double,
	last_datetime	string
)
""")

#create table of  brand feature
# create brandFeature table
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

#  submit predict area
# create brandFeature table
sql("""
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

	buy1	 double,
	buy3	 double,
	buy5	 double,
	buy7	 double,
	buy10	 double,
	buy15	 double,
	buy25	 double,

	collect1	double ,
	collect3	 double,
	collect5	 double,
	collect7	 double,
	collect10	double,
	collect15	 double,
	collect25	 double,

	basket1		 double,
	basket3		 double,
	basket5		 double,
	basket7		 double,
	basket10	     	double,
	basket15		 double,
	basket25		double,

     click_this		double,
	buy_this    	double,
	collect_this 	 double,
	basket_this	  	double,
	action_this		double,
	last_datetime	string
)
""")
sql("""
create table  lmn_b_or_u_features_6_11_8_15_temp
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
drop table if exists  lmn_train_area_validate_gbrt_model_output_scale ;
create table   lmn_train_area_validate_gbrt_model_output_scale
(
	user_id	string ,
	brand_id	string,
	last_datetime	string,
	var		double,
	scalepara	double,
	var_scaled  double
)
""")


""")
