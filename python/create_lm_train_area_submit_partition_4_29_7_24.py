


#####  brand_features_temp  ==>  brand_features      #################
sql("""
drop table if exists   lm_brand_features_4_29_6_24  ;
create table  lm_brand_features_4_29_6_24  as
select 
	brand_id,
	count(*)  asked_users ,
	sum(clicked) 	n_clicked,
	sum(bought)		n_bought,
	sum(collected)	n_collected,
	sum(basketed)	n_basketed,
	sum(actions)	actions_by 
from 
  lm_brand_features_4_29_6_24_temp
group by brand_id;

""")
######################################



#connect all
sql("""
drop table if  exists lm_train_area_submit_partition_4_29_7_24 ;
create table lm_train_area_submit_partition_4_29_7_24 as
select 
	lm_user_brand_features_4_29_6_24 .user_id,
      lm_user_brand_features_4_29_6_24 .brand_id,
	click1	 ,
	click3	 ,
	click5	 ,
	click7	 ,
	click15	 ,
	click25	 ,

	buy1	 ,
	buy3	 ,
	buy5	 ,
	buy7	 ,
	buy15	 ,
	buy25	 ,

	collect1	 ,
	collect3	 ,
	collect5	 ,
	collect7	 ,
	collect15	 ,
	collect25	 ,

	basket1	 ,
	basket3	 ,
	basket5	 ,
	basket7	 ,
	basket15	 ,
	basket25	,

	ask_brands   ,
	n_click	 ,
	n_buy		 ,
	n_collect	 ,
	n_basket	 ,
	n_actions	  ,

	asked_users ,
   	n_clicked,
 	n_bought,
	n_collected,
	n_basketed,
	actions_by ,

      buy_brand	,
	case
	when buy_brand is not null then 1
	when buy_brand is null then 0
	end as buy_label
from
 lm_user_brand_features_4_29_6_24 

left outer join
lm_user_features_4_29_6_24
on 	lm_user_brand_features_4_29_6_24 .user_id = lm_user_features_4_29_6_24 .user_id

left outer join
lm_brand_features_4_29_6_24 
on 	lm_user_brand_features_4_29_6_24 .brand_id = lm_brand_features_4_29_6_24 .brand_id		


left outer join(
   select
   user_id,
   brand_id  buy_brand
from(
	select user_id,brand_id 
	from t_alibaba_bigdata_user_brand_total_1
	where type = '1' and  visit_datetime>='06-25' and   visit_datetime <= '07-24'   -- 
	group by  user_id , brand_id
    )label
) buy_label_table_6_25_7_24
on 	lm_user_brand_features_4_29_6_24 .user_id = buy_label_table_6_25_7_24.user_id and 	lm_user_brand_features_4_29_6_24 .brand_id = buy_label_table_6_25_7_24.buy_brand

where n_actions is not null and  actions_by is not null   ; --fliter
""")











