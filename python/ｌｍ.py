

sql("""
drop table if exists lm_small_sample_of_u_b_information;
create table lm_small_sample_of_u_b_information as
select *
from
yi_user_brand_information limit 1000;
""")




sql("""
-- 
drop table if exists lm_user_fre_value ;
create table lm_user_fre_value as
select 
  user_id,
  sum(n_click)+sum(n_buy)+sum(n_collect)+sum(n_basket) as actions,
  sum(n_click)/(sum(n_click)+sum(n_buy)+sum(n_collect)+sum(n_basket)) as fre_click,
  sum(n_buy)/(sum(n_click)+sum(n_buy)+sum(n_collect)+sum(n_basket)) as fre_buy
 from 
  lm_small_sample_of_u_b_information 
  group by user_id;
""")



sql("""
-- connect 
drop table if exists lm_user_brand_fre_for_mr ;
create table lm_user_brand_fre_for_mr as
select
lm_small_sample_of_u_b_information.user_id,
lm_small_sample_of_u_b_information.brand_id,
n_click, n_buy, n_collect, n_basket,
fre_click,
fre_buy,
actions
from
lm_small_sample_of_u_b_information 
left outer join
lm_user_fre_value
on lm_small_sample_of_u_b_information.user_id = lm_user_fre_value.user_id 
where actions >=7 ;-- fliter
""")


sql("""
create table lm_score_output_all
(
     user_id string,
    brand_id string,
     score double
)
""")


sql("""
drop table	 if exists  lm_lr_test_train;
create table lm_lr_test_train
(
	user_id	 string,
	brand_id	string,
	w0		double,
	w1		double,
	w2		double,
	w3		double,
	w4		double,

	e_in		double
)
""")

# 
sql("""
drop table if exists lm_validate_set;
create table lm_validate_set as
select user_id,wm_concat(',',brand_id) as brand
from (select user_id,brand_id
 	from  validate_set
 	)a
group by user_id;
""")








# create table of user_brand  feature
sql("""
create table lm_user_brand_feature_4_25_6_15
(
	user_id	string,
	brand_id	string,

	click1	double,
	click3	double,
	click5	double,
	click7	double,
	click15	double,
	click25	double,

	buy1	double,
	buy3	double,
	buy5	double,
	buy7	double,
	buy15	double,
	buy25	double,

	collect1	double,
	collect3	double,
	collect5	double,
	collect7	double,
	collect15	double,
	collect25	double,

	basket1	double,
	basket3	double,
	basket5	double,
	basket7	double,
	basket15	double,
	basket25	double
)
""")

#create table of  user feature
# create userFeature table
sql("""
drop table if exists lm_user_feature_4_25_6_15;
create table  lm_user_feature_4_25_6_15
(
	user_id	string,
	ask_brands  double,
	n_click	double,
	n_buy		double,
	n_collect	double,
	n_basket	double,
	n_actions	double
)
""")

#################  ALL  #############

# create table of user_brand  feature
sql("""
create table lm_user_brand_feature_5_25_7_15
(
	user_id	string,
	brand_id	string,

	click1	double,
	click3	double,
	click5	double,
	click7	double,
	click15	double,
	click25	double,

	buy1	double,
	buy3	double,
	buy5	double,
	buy7	double,
	buy15	double,
	buy25	double,

	collect1	double,
	collect3	double,
	collect5	double,
	collect7	double,
	collect15	double,
	collect25	double,

	basket1	double,
	basket3	double,
	basket5	double,
	basket7	double,
	basket15	double,
	basket25	double
)
""")

#create table of  user feature
# create userFeature table
sql("""
drop table if exists lm_user_features_4_13_6_08;
create table  lm_user_features_4_13_6_08
(
	user_id	string,
	ask_brands  double,
	n_click	double,
	n_buy		double,
	n_collect	double,
	n_basket	double,
	n_actions	double
)
""")


 ###     pre from here with model ##########
# create table of user_brand  feature
sql("""
create table lm_user_brand_features_5_13_7_08
(
	user_id	string,
	brand_id	string,

	click1	double,
	click3	double,
	click5	double,
	click7	double,
	click15	double,
	click25	double,

	buy1	double,
	buy3	double,
	buy5	double,
	buy7	double,
	buy15	double,
	buy25	double,

	collect1	double,
	collect3	double,
	collect5	double,
	collect7	double,
	collect15	double,
	collect25	double,

	basket1	double,
	basket3	double,
	basket5	double,
	basket7	double,
	basket15	double,
	basket25	double
)
""")


#create table of  user feature
# create userFeature table
sql("""
drop table if exists lm_user_features_5_13_7_08;
create table  lm_user_features_5_13_7_08
(
	user_id	string,
	ask_brands  double,
	n_click	double,
	n_buy		double,
	n_collect	double,
	n_basket	double,
	n_actions	double
)
""")


#create table of  brand feature
# create brandFeature table
sql("""
drop table if exists lm_brand_features_5_13_7_08_temp ;
create table  lm_brand_features_5_13_7_08_temp
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






