

sql("""

create table predict_label_6_16_7_15 as
select
user_id,
brand_id,
sqrt ( cast( count(*) as double))  label_val
from  t_alibaba_bigdata_user_brand_total_1
	where type = '1' and  visit_datetime>='06-16' and   visit_datetime <= '07-15'
group by user_id ,brand_id;

""")




sql("""
drop table if  exists  lmn_b_date_features_4_11_6_15 ;
create table lmn_b_date_features_4_11_6_15 as
	select 
	brand_id ,	

 	sum(click1)  bclicked1,
	sum(click3)  bclicked3,
	sum(click5)  bclicked5,
	sum(click7)  bclicked7,
	sum(click10)  bclicked10,
	sum(click15)  bclicked15,
	sum(click25)  bclicked25,

      sum(buy1)   bbought1,
      sum(buy3)   bbought3,
      sum(buy5)   bbought5,
      sum(buy7)   bbought7,
      sum(buy10)   bbought10,
      sum(buy15)   bbought15,
      sum(buy25)   bbought25,

	sum(collect1)  bcollected1,
	sum(collect3)  bcollected3,
	sum(collect5)  bcollected5,
	sum(collect7)  bcollected7,
	sum(collect10)  bcollected10,
	sum(collect15)  bcollected15,
	sum(collect25)  bcollected25,

	sum(basket1)  bbasketed1,
	sum(basket3)  bbasketed3,
	sum(basket5)  bbasketed5,
	sum(basket7)  bbasketed7,
	sum(basket10)  bbasketed10,
	sum(basket15)  bbasketed15,
	sum(basket25)  bbasketed25

from lmn_user_brand_features_4_11_6_15
group by brand_id;

""")




sql("""
drop table if  exists  lmn_b_date_features_5_11_7_15 ;
create table lmn_b_date_features_5_11_7_15 as
	select 
	brand_id ,	

 	sum(click1)  bclicked1,
	sum(click3)  bclicked3,
	sum(click5)  bclicked5,
	sum(click7)  bclicked7,
	sum(click10)  bclicked10,
	sum(click15)  bclicked15,
	sum(click25)  bclicked25,

      sum(buy1)   bbought1,
      sum(buy3)   bbought3,
      sum(buy5)   bbought5,
      sum(buy7)   bbought7,
      sum(buy10)   bbought10,
      sum(buy15)   bbought15,
      sum(buy25)   bbought25,

	sum(collect1)  bcollected1,
	sum(collect3)  bcollected3,
	sum(collect5)  bcollected5,
	sum(collect7)  bcollected7,
	sum(collect10)  bcollected10,
	sum(collect15)  bcollected15,
	sum(collect25)  bcollected25,

	sum(basket1)  bbasketed1,
	sum(basket3)  bbasketed3,
	sum(basket5)  bbasketed5,
	sum(basket7)  bbasketed7,
	sum(basket10)  bbasketed10,
	sum(basket15)  bbasketed15,
	sum(basket25)  bbasketed25

from lmn_user_brand_features_5_11_7_15
group by brand_id;

""")






sql("""
drop table if  exists  lmn_b_date_features_6_11_8_15 ;
create table lmn_b_date_features_6_11_8_15 as
	select 
	brand_id ,	

	sum(click1)  bclicked1,
	sum(click3)  bclicked3,
	sum(click5)  bclicked5,
	sum(click7)  bclicked7,
	sum(click10)  bclicked10,
	sum(click15)  bclicked15,
	sum(click25)  bclicked25,

      sum(buy1)   bbought1,
      sum(buy3)   bbought3,
      sum(buy5)   bbought5,
      sum(buy7)   bbought7,
      sum(buy10)   bbought10,
      sum(buy15)   bbought15,
      sum(buy25)   bbought25,

	sum(collect1)  bcollected1,
	sum(collect3)  bcollected3,
	sum(collect5)  bcollected5,
	sum(collect7)  bcollected7,
	sum(collect10)  bcollected10,
	sum(collect15)  bcollected15,
	sum(collect25)  bcollected25,

	sum(basket1)  bbasketed1,
	sum(basket3)  bbasketed3,
	sum(basket5)  bbasketed5,
	sum(basket7)  bbasketed7,
	sum(basket10)  bbasketed10,
	sum(basket15)  bbasketed15,
	sum(basket25)  bbasketed25

from lmn_user_brand_features_6_11_8_15
group by brand_id;

""")



#################################################################




sql("""
drop table if exists  lmn_brand_features_4_11_6_15_bdf ;
create table  lmn_brand_features_4_11_6_15_bdf as
select 
       lmn_brand_features_4_11_6_15_t.brand_id , 
 	n_clicked/actions_by 	n_clicked_ratio,
	n_bought/actions_by	n_bought_ratio,
	n_collected/actions_by	n_collected_ratio,	
	n_basketed/actions_by	n_basketed_ratio,	
	actions_by ,
	clicked_dist/users_dist 		clicked_dist_ratio,
	bought_dist/users_dist 			bought_dist_ratio,
	collected_dist/users_dist		collected_dist_ratio,
	basketed_dist/users_dist		basketed_dist_ratio ,
     	 users_dist         users_dist ,

	 bclicked1 /actions_by   clicked_ratio1  ,
  	bclicked3/actions_by   clicked_ratio3 ,
  	bclicked5/actions_by   clicked_ratio5 ,
  	bclicked7/actions_by   clicked_ratio7 ,
	bclicked10/actions_by   clicked_ratio10 ,
  	bclicked15/actions_by   clicked_ratio15 ,
  	bclicked25/actions_by   clicked_ratio25 ,

       bbought1/actions_by  bought_ratio1  ,
       bbought3/actions_by  bought_ratio3 ,
   	  bbought5/actions_by  bought_ratio5 ,
       bbought7/actions_by  bought_ratio7 ,
       bbought10/actions_by  bought_ratio10 ,
	bbought15/actions_by  bought_ratio15 ,
       bbought25/actions_by  bought_ratio25 ,

 bcollected1/actions_by  collected_ratio1  ,
 bcollected3/actions_by  collected_ratio3 ,
  bcollected5/actions_by  collected_ratio5 ,
  bcollected7/actions_by  collected_ratio7 ,
  bcollected10/actions_by  collected_ratio10 ,
bcollected15/actions_by  collected_ratio15 ,
  bcollected25/actions_by  collected_ratio25 ,

 bbasketed1/actions_by  basketed_ratio1  ,
  bbasketed3/actions_by  basketed_ratio3 ,
  bbasketed5/actions_by  basketed_ratio5 ,
  bbasketed7/actions_by  basketed_ratio7 ,
  bbasketed10/actions_by  basketed_ratio10 ,
bbasketed15/actions_by  basketed_ratio15 ,
  bbasketed25/actions_by  basketed_ratio25 


from
lmn_brand_features_4_11_6_15_t
left outer join
lmn_b_date_features_4_11_6_15
on  lmn_brand_features_4_11_6_15_t.brand_id = lmn_b_date_features_4_11_6_15.brand_id

""")

sql("""
drop table if exists  lmn_brand_features_5_11_7_15_bdf ;
create table  lmn_brand_features_5_11_7_15_bdf as
select 
       lmn_brand_features_5_11_7_15_t.brand_id , 
 	n_clicked/actions_by 	n_clicked_ratio,
	n_bought/actions_by	n_bought_ratio,
	n_collected/actions_by	n_collected_ratio,	
	n_basketed/actions_by	n_basketed_ratio,	
	actions_by ,
	clicked_dist/users_dist 		clicked_dist_ratio,
	bought_dist/users_dist 			bought_dist_ratio,
	collected_dist/users_dist		collected_dist_ratio,
	basketed_dist/users_dist		basketed_dist_ratio ,
     	 users_dist         users_dist,


	 	 bclicked1 /actions_by   clicked_ratio1  ,
  	bclicked3/actions_by   clicked_ratio3 ,
  	bclicked5/actions_by   clicked_ratio5 ,
  	bclicked7/actions_by   clicked_ratio7 ,
	bclicked10/actions_by   clicked_ratio10 ,
  	bclicked15/actions_by   clicked_ratio15 ,
  	bclicked25/actions_by   clicked_ratio25 ,

       bbought1/actions_by  bought_ratio1  ,
       bbought3/actions_by  bought_ratio3 ,
   	  bbought5/actions_by  bought_ratio5 ,
       bbought7/actions_by  bought_ratio7 ,
       bbought10/actions_by  bought_ratio10 ,
	bbought15/actions_by  bought_ratio15 ,
       bbought25/actions_by  bought_ratio25 ,

 bcollected1/actions_by  collected_ratio1  ,
 bcollected3/actions_by  collected_ratio3 ,
  bcollected5/actions_by  collected_ratio5 ,
  bcollected7/actions_by  collected_ratio7 ,
  bcollected10/actions_by  collected_ratio10 ,
bcollected15/actions_by  collected_ratio15 ,
  bcollected25/actions_by  collected_ratio25 ,

 bbasketed1/actions_by  basketed_ratio1  ,
  bbasketed3/actions_by  basketed_ratio3 ,
  bbasketed5/actions_by  basketed_ratio5 ,
  bbasketed7/actions_by  basketed_ratio7 ,
  bbasketed10/actions_by  basketed_ratio10 ,
bbasketed15/actions_by  basketed_ratio15 ,
  bbasketed25/actions_by  basketed_ratio25 


from
lmn_brand_features_5_11_7_15_t
left outer join
lmn_b_date_features_5_11_7_15
on  lmn_brand_features_5_11_7_15_t.brand_id = lmn_b_date_features_5_11_7_15.brand_id

""")




sql("""
drop table if exists  lmn_brand_features_6_11_8_15_bdf ;
create table  lmn_brand_features_6_11_8_15_bdf as
select 
       lmn_brand_features_6_11_8_15_t.brand_id , 
 	n_clicked/actions_by 	n_clicked_ratio,
	n_bought/actions_by	n_bought_ratio,
	n_collected/actions_by	n_collected_ratio,	
	n_basketed/actions_by	n_basketed_ratio,	
	actions_by ,
	clicked_dist/users_dist 		clicked_dist_ratio,
	bought_dist/users_dist 			bought_dist_ratio,
	collected_dist/users_dist		collected_dist_ratio,
	basketed_dist/users_dist		basketed_dist_ratio ,
     	 users_dist         users_dist,


	 	 bclicked1 /actions_by   clicked_ratio1  ,
  	bclicked3/actions_by   clicked_ratio3 ,
  	bclicked5/actions_by   clicked_ratio5 ,
  	bclicked7/actions_by   clicked_ratio7 ,
	bclicked10/actions_by   clicked_ratio10 ,
  	bclicked15/actions_by   clicked_ratio15 ,
  	bclicked25/actions_by   clicked_ratio25 ,

       bbought1/actions_by  bought_ratio1  ,
       bbought3/actions_by  bought_ratio3 ,
   	  bbought5/actions_by  bought_ratio5 ,
       bbought7/actions_by  bought_ratio7 ,
       bbought10/actions_by  bought_ratio10 ,
	bbought15/actions_by  bought_ratio15 ,
       bbought25/actions_by  bought_ratio25 ,

 bcollected1/actions_by  collected_ratio1  ,
 bcollected3/actions_by  collected_ratio3 ,
  bcollected5/actions_by  collected_ratio5 ,
  bcollected7/actions_by  collected_ratio7 ,
  bcollected10/actions_by  collected_ratio10 ,
bcollected15/actions_by  collected_ratio15 ,
  bcollected25/actions_by  collected_ratio25 ,

 bbasketed1/actions_by  basketed_ratio1  ,
  bbasketed3/actions_by  basketed_ratio3 ,
  bbasketed5/actions_by  basketed_ratio5 ,
  bbasketed7/actions_by  basketed_ratio7 ,
  bbasketed10/actions_by  basketed_ratio10 ,
bbasketed15/actions_by  basketed_ratio15 ,
  bbasketed25/actions_by  basketed_ratio25 

from
lmn_brand_features_6_11_8_15_t
left outer join
lmn_b_date_features_6_11_8_15
on lmn_brand_features_6_11_8_15_t.brand_id = lmn_b_date_features_6_11_8_15.brand_id

""")


#############

sql("""
create table lmn_train_area_submit_5_11_8_15_bdf_lab1  as
select * 
from lmn_train_area_submit_5_11_8_15_bdf
where  buy_label >=1 ;

create table lmn_train_area_submit_5_11_8_15_bdf_lab0  as
select * 
from  lmn_train_area_submit_5_11_8_15_bdf
where  buy_label =0 ;
""")

table_label1 = Table("lmn_train_area_submit_5_11_8_15_bdf_lab1") ;
Size = table_label1.getRecordCount();
print Size ;
sampleSize  =  10 * Size ;
print sampleSize;
 DataProc.Sample.randomSample( "lmn_train_area_submit_5_11_8_15_bdf_lab0",int(sampleSize) ,
			"lmn_train_area_submit_5_11_8_15_bdf_lab0_sample") ;


sql("""
drop table if exists   lmn_train_area_submit_5_11_8_15_bdf_lab01_union ;
create table  lmn_train_area_submit_5_11_8_15_bdf_lab01_union   as
select * 
from(
select * from  lmn_train_area_submit_5_11_8_15_bdf_lab1
union all
select * from  lmn_train_area_submit_5_11_8_15_bdf_lab0_sample
)t ;
""")






##  gbrt
sql(""" drop table if exists lmn_train_area_submit_lab01_union_sm_gbrt_model_output ; """)
DataProc.appendColumns (['lmn_predict_area_submit_6_11_8_15_bdf',
				'lmn_train_area_submit_5_11_8_15_bdf_lab01_union_sm_gbrt_model_yvar'],
				'lmn_train_area_submit_lab01_union_sm_gbrt_model_output')



###########################################################

// get top   2835057
sql(""" drop table if exists lmn_train_area_submit_output_2835057 ;""")
 DataProc.topn( "lmn_train_area_submit_lab01_union_sm_gbrt_model_output",
		    2835057,
		    "lmn_train_area_submit_output_2835057",
		   ["y_var"],
		    ["-"] );
## output 

###  random tree 

sql("""
drop table if exists t_tmall_add_user_brand_predict_dh ; --
create table t_tmall_add_user_brand_predict_dh as  --
select --  first
    user_id, 
    wm_concat(',',brand_id) as brand -- connect the result
from
   ( select
	user_id ,
   	brand_id 
  from   lmn_train_area_submit_output_2835057   --
 
) a 
group by user_id;  
""")





