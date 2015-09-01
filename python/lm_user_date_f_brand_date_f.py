
sql("""
drop table if  exists  lm_user_date_features_5_20_7_15 ;
create table lm_user_date_features_5_20_7_15 as
	select 
	user_id ,	

	sum(click1) uclick1,
	sum(click3) uclick3,
	sum(click5) uclick5,
	sum(click7) uclick7,
	sum(click15) uclick15,
	sum(click25) uclick25,

      sum(buy1)  ubuy1,
      sum(buy3)  ubuy3,
      sum(buy5)  ubuy5,
      sum(buy7)  ubuy7,
      sum(buy15)  ubuy15,
      sum(buy25)  ubuy25,

	sum(collect1) ucollect1,
	sum(collect3) ucollect3,
	sum(collect5) ucollect5,
	sum(collect7) ucollect7,
	sum(collect15) ucollect15,
	sum(collect25) ucollect25,

	sum(basket1) ubasket1,
	sum(basket3) ubasket3,
	sum(basket5) ubasket5,
	sum(basket7) ubasket7,
	sum(basket15) ubasket15,
	sum(basket25) ubasket25

from lm_user_brand_features_5_20_7_15
group by user_id;

""")

#################################################################################################



#################################################################################################

sql("""
drop table if  exists  lm_brand_date_features_5_20_7_15 ;
create table lm_brand_date_features_5_20_7_15 as
	select 
	brand_id ,	

	sum(click1) bclicked1,
	sum(click3) bclicked3,
	sum(click5) bclicked5,
	sum(click7) bclicked7,
	sum(click15) bclicked15,
	sum(click25) bclicked25,

      sum(buy1)  bbought1,
      sum(buy3)  bbought3,
      sum(buy5)  bbought5,
      sum(buy7)  bbought7,
      sum(buy15)  bbought15,
      sum(buy25)  bbought25,

	sum(collect1) bcollected1,
	sum(collect3) bcollected3,
	sum(collect5) bcollected5,
	sum(collect7) bcollected7,
	sum(collect15) bcollected15,
	sum(collect25) bcollected25,

	sum(basket1) bbasketed1,
	sum(basket3) bbasketed3,
	sum(basket5) bbasketed5,
	sum(basket7) bbasketed7,
	sum(basket15) bbasketed15,
	sum(basket25) bbasketed25

from lm_user_brand_features_5_20_7_15
group by brand_id;

""")



##############################################


##############################################
sql("""
drop table if  exists  lm_user_date_features_4_20_6_15 ;
create table lm_user_date_features_4_20_6_15 as
	select 
	user_id ,	

	sum(click1) uclick1,
	sum(click3) uclick3,
	sum(click5) uclick5,
	sum(click7) uclick7,
	sum(click15) uclick15,
	sum(click25) uclick25,

      sum(buy1)  ubuy1,
      sum(buy3)  ubuy3,
      sum(buy5)  ubuy5,
      sum(buy7)  ubuy7,
      sum(buy15)  ubuy15,
      sum(buy25)  ubuy25,

	sum(collect1) ucollect1,
	sum(collect3) ucollect3,
	sum(collect5) ucollect5,
	sum(collect7) ucollect7,
	sum(collect15) ucollect15,
	sum(collect25) ucollect25,

	sum(basket1) ubasket1,
	sum(basket3) ubasket3,
	sum(basket5) ubasket5,
	sum(basket7) ubasket7,
	sum(basket15) ubasket15,
	sum(basket25) ubasket25

from lm_user_brand_features_4_20_6_15
group by user_id;

""")

#########################################################

sql("""
drop table if  exists  lm_brand_date_features_4_20_6_15 ;
create table lm_brand_date_features_4_20_6_15 as
	select 
	brand_id ,	

	sum(click1) bclicked1,
	sum(click3) bclicked3,
	sum(click5) bclicked5,
	sum(click7) bclicked7,
	sum(click15) bclicked15,
	sum(click25) bclicked25,

      sum(buy1)  bbought1,
      sum(buy3)  bbought3,
      sum(buy5)  bbought5,
      sum(buy7)  bbought7,
      sum(buy15)  bbought15,
      sum(buy25)  bbought25,

	sum(collect1) bcollected1,
	sum(collect3) bcollected3,
	sum(collect5) bcollected5,
	sum(collect7) bcollected7,
	sum(collect15) bcollected15,
	sum(collect25) bcollected25,

	sum(basket1) bbasketed1,
	sum(basket3) bbasketed3,
	sum(basket5) bbasketed5,
	sum(basket7) bbasketed7,
	sum(basket15) bbasketed15,
	sum(basket25) bbasketed25

from lm_user_brand_features_4_20_6_15
group by brand_id;

""")






