 MR ...
##   brand_featers_temp  == > brand_features_extend  ###
sql("""
drop table if exists  lmn_u_or_b_features_4_11_6_15_extend ;
create table lmn_u_or_b_features_4_11_6_15_extend  as
select 
	brand_id,
	user_id ,

	clicked ,
 	bought  ,
	collected ,
	basketed ,
	case
	when clicked > 0 then   1
	when clicked  = 0 then  0
	end as clicked_flag ,
	case
	when bought > 0 then	1  
	when bought  = 0 then   0
	end as bought_flag ,
	case
	when collected > 0 then	  1 
	when collected  = 0 then  0
	end as collected_flag ,
	case
	when basketed > 0 then   1
	when basketed  = 0 then  0
	end as basketed_flag ,

	actions 
from 
lmn_u_or_b_features_4_11_6_15_temp
""")

#####  brand_features_extend  ==>  brand_features      #################
sql("""
drop table if exists  lmn_brand_features_4_11_6_15_t ;
create table  lmn_brand_features_4_11_6_15_t  as
select 
	brand_id,
	cast( count(*) as double)  users_dist ,
	sum(clicked) 	n_clicked,
	sum(bought)		n_bought,
	sum(collected)	n_collected,
	sum(basketed)	n_basketed,
	sum(actions)	actions_by ,

	cast( sum(clicked_flag) as double) 	clicked_dist,
	cast( sum(bought_flag)	as double)	bought_dist,
	cast( sum(collected_flag) as double)	collected_dist,
	cast( sum(basketed_flag) as double)	basketed_dist

from 
  lmn_u_or_b_features_4_11_6_15_extend
group by brand_id;
""")
######################################
##  brand_features_extend == > user_features 
sql("""
drop table if exists  lmn_user_features_4_11_6_15_t ;
create table  lmn_user_features_4_11_6_15_t  as
select 
	user_id,
	cast( count(*) as double)  brand_dist ,
	sum(clicked) 	n_click,
	sum(bought)		n_buy,
	sum(collected)	n_collect,
	sum(basketed)	n_basket,
	sum(actions)	actions_sum ,

	cast( sum(clicked_flag) as double) 		click_dist,
	cast( sum(bought_flag) as double)		buy_dist,
	cast( sum(collected_flag) as double)	collect_dist,
	cast( sum(basketed_flag) as double)		basket_dist
from 
lmn_u_or_b_features_4_11_6_15_extend
group by user_id;
""")
##################################################
##################################################
sql("""
drop table if exists  lmn_u_or_b_features_5_11_7_15_extend ;
create table  lmn_u_or_b_features_5_11_7_15_extend  as
select 
	brand_id,
	user_id ,

	clicked ,
 	bought  ,
	collected ,
	basketed ,
	case
	when clicked > 0 then   1
	when clicked  = 0 then  0
	end as clicked_flag ,
	case
	when bought > 0 then	1  
	when bought  = 0 then   0
	end as bought_flag ,
	case
	when collected > 0 then	  1 
	when collected  = 0 then  0
	end as collected_flag ,
	case
	when basketed > 0 then   1
	when basketed  = 0 then  0
	end as basketed_flag ,

	actions 
from 
 lmn_u_or_b_features_5_11_7_15_temp
""")

#####  brand_features_extend  ==>  brand_features      #################
sql("""
drop table if exists  lmn_brand_features_5_11_7_15_t ;
create table  lmn_brand_features_5_11_7_15_t  as
select 
	brand_id,
	cast( count(*) as double)  users_dist ,
	sum(clicked) 	n_clicked,
	sum(bought)		n_bought,
	sum(collected)	n_collected,
	sum(basketed)	n_basketed,
	sum(actions)	actions_by ,

	cast( sum(clicked_flag) as double) 	clicked_dist,
	cast( sum(bought_flag)	as double)	bought_dist,
	cast( sum(collected_flag) as double)	collected_dist,
	cast( sum(basketed_flag) as double)	basketed_dist

from 
lmn_u_or_b_features_5_11_7_15_extend
group by brand_id;
""")
######################################
##  brand_features_extend == > user_features 
sql("""
drop table if exists  lmn_user_features_5_11_7_15_t ;
create table  lmn_user_features_5_11_7_15_t  as
select 
	user_id,
	cast( count(*) as double)  brand_dist ,
	sum(clicked) 	n_click,
	sum(bought)		n_buy,
	sum(collected)	n_collect,
	sum(basketed)	n_basket,
	sum(actions)	actions_sum ,

	cast( sum(clicked_flag) as double) 		click_dist,
	cast( sum(bought_flag) as double)		buy_dist,
	cast( sum(collected_flag) as double)	collect_dist,
	cast( sum(basketed_flag) as double)		basket_dist

from 
lmn_u_or_b_features_5_11_7_15_extend
group by user_id;
""")



###################################################################################################
###################################################################################################

#  connect  

###################################################################################################
###################################################################################################
#connect all ,get train table for validate

sql("""
drop table if  exists lmn_train_area_validate_4_11_7_15  ;
create table lmn_train_area_validate_4_11_7_15 as
select
	user_id,
      brand_id,
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
	 action_this	,

 -- brand features
  b_click1 ,
      b_click3 ,
      b_click5 ,
	 b_click7 ,
	 b_click10 ,
	 b_click15 ,
	 b_click25 ,

	 b_buy1	 ,
	 b_buy3	 ,
	 b_buy5	 ,
	 b_buy7	 ,
	 b_buy10	,
	 b_buy15	 ,
	 b_buy25	 ,

	 b_collect1	 ,
	 b_collect3	 ,
	 b_collect5	 ,
	 b_collect7	 ,
	 b_collect10	,
	 b_collect15 ,
	 b_collect25 ,

	 b_basket1	 ,
	 b_basket3	 ,
	 b_basket5 ,
	 b_basket7	 ,
	 b_basket10	,
	 b_basket15	 ,
	 b_basket25  ,
      --ratio
       b_ratio_click1,
        b_ratio_click3,
        b_ratio_click5,
 	  b_ratio_click7,
	    b_ratio_click10,
	   b_ratio_click15,
         b_ratio_click25,

	  b_ratio_buy1	 ,
 	  b_ratio_buy3	 ,
	  b_ratio_buy5	 ,
	  b_ratio_buy7	 ,
	 b_ratio_buy10	,
	  b_ratio_buy15	 ,
	 b_ratio_buy25	 ,

	  b_ratio_collect1,
	  b_ratio_collect3,
	  b_ratio_collect5,
	  b_ratio_collect7,
	   b_ratio_collect10,
     b_ratio_collect15,
     b_ratio_collect25,

    b_ratio_basket1	 ,
   b_ratio_basket3	 ,
   b_ratio_basket5 ,
   b_ratio_basket7	 ,
   b_ratio_basket10	,
   b_ratio_basket15	 ,
     b_ratio_basket25 ,

      --
  	n_clicked_ratio,
	n_bought_ratio,
	n_collected_ratio,	
	n_basketed_ratio,	
	actions_by ,

	clicked_dist_ratio,
	bought_dist_ratio,
	collected_dist_ratio,
	basketed_dist_ratio ,
      users_dist*1.0  users_dist ,


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
	brand_dist*1.0  brand_dist  ,
	
	last_datetime ,			

	--label
      buy_brand	,
	case
	when buy_brand is not null then 1
	when buy_brand is null then 0
	end as buy_label
from
 lmn_user_brand_features_4_11_6_15

left outer join
lmn_user_features_4_11_6_15
on 	lmn_user_brand_features_4_11_6_15.user_id = lmn_user_features_4_11_6_15.user_id

left outer join
lmn_brand_features_4_11_6_15
on 	lmn_user_brand_features_4_11_6_15.brand_id = lmn_brand_features_4_11_6_15.brand_id		

left outer join(
   select
   user_id,
   brand_id  buy_brand
from(
	select user_id,brand_id 
	from t_alibaba_bigdata_user_brand_total_1
	where type = '1' and  visit_datetime>='06-16' and   visit_datetime <= '07-15'
	group by  user_id , brand_id
    )label
) buy_label_table_6_16_7_15
on 	lmn_user_brand_features_4_11_6_15.user_id = buy_label_table_6_16_7_15.user_id and 	lmn_user_brand_features_4_11_6_15.brand_id = buy_label_table_6_16_7_15.buy_brand
where actions_sum is not null and  actions_by is not null   ; --fliter
""")


############################################################################################################################################################


#connect all ,get train table for validate
sql("""
drop table if  exists lmn_predict_area_validate_5_11_7_15  ;
create table lmn_predict_area_validate_5_11_7_15 as
select 
	lmn_user_brand_features_5_11_7_15.user_id,
      lmn_user_brand_features_5_11_7_15.brand_id,
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
	 action_this	,

 -- brand features
  b_click1 ,
      b_click3 ,
      b_click5 ,
	 b_click7 ,
	 b_click10 ,
	 b_click15 ,
	 b_click25 ,

	 b_buy1	 ,
	 b_buy3	 ,
	 b_buy5	 ,
	 b_buy7	 ,
	 b_buy10	,
	 b_buy15	 ,
	 b_buy25	 ,

	 b_collect1	 ,
	 b_collect3	 ,
	 b_collect5	 ,
	 b_collect7	 ,
	 b_collect10	,
	 b_collect15 ,
	 b_collect25 ,

	 b_basket1	 ,
	 b_basket3	 ,
	 b_basket5 ,
	 b_basket7	 ,
	 b_basket10	,
	 b_basket15	 ,
	 b_basket25  ,
      --ratio
       b_ratio_click1,
        b_ratio_click3,
        b_ratio_click5,
 	  b_ratio_click7,
	    b_ratio_click10,
	   b_ratio_click15,
         b_ratio_click25,

	  b_ratio_buy1	 ,
 	  b_ratio_buy3	 ,
	  b_ratio_buy5	 ,
	  b_ratio_buy7	 ,
	 b_ratio_buy10	,
	  b_ratio_buy15	 ,
	 b_ratio_buy25	 ,

	  b_ratio_collect1,
	  b_ratio_collect3,
	  b_ratio_collect5,
	  b_ratio_collect7,
	   b_ratio_collect10,
     b_ratio_collect15,
     b_ratio_collect25,

    b_ratio_basket1	 ,
   b_ratio_basket3	 ,
   b_ratio_basket5 ,
   b_ratio_basket7	 ,
   b_ratio_basket10	,
   b_ratio_basket15	 ,
     b_ratio_basket25 ,

      --
  	n_clicked_ratio,
	n_bought_ratio,
	n_collected_ratio,	
	n_basketed_ratio,	
	actions_by ,

	clicked_dist_ratio,
	bought_dist_ratio,
	collected_dist_ratio,
	basketed_dist_ratio ,
      cast( users_dist as double)   user_dist,

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
	cast( brand_dist as double)  brand_dist,
	
	last_datetime		
from
 lmn_user_brand_features_5_11_7_15

left outer join
lmn_user_features_5_11_7_15
on 	lmn_user_brand_features_5_11_7_15.user_id = lmn_user_features_5_11_7_15.user_id

left outer join
lmn_brand_features_5_11_7_15
on 	lmn_user_brand_features_5_11_7_15.brand_id = lmn_brand_features_5_11_7_15.brand_id		

where actions_sum is not null and  actions_by is not null   ; --fliter
""")
