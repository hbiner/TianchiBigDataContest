
# create table of user_brand  features
sql("""
create table user_brand_feature_period
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


# create userFeatures table
sql("""
drop table if exists user_features_start_end
create table  user_features_start_end
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




## create  brand_features_temp table
sql("""
drop table if exists brand_features_temp ;
create table  brand_features_temp
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


##  brand features temp ==> brand features   
sql("""
drop table if exists  brand_features;
create table  brand_features  as
select 
	brand_id,
	count(*)  asked_users ,
	sum(clicked) 	n_clicked,
	sum(bought)		n_bought,
	sum(collected)	n_collected,
	sum(basketed)	n_basketed,
	sum(actions)	actions_by 
from 
 brand_features_temp
group by brand_id;
""")


