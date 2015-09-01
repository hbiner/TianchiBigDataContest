
#connect all
sql("""
drop table if  exists lm_predict_area_validate_5_20_7_15  ;
create table lm_predict_area_validate_5_20_7_15 as
select 
	lm_user_brand_features_5_20_7_15.user_id,
      lm_user_brand_features_5_20_7_15.brand_id,
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

	uclick1,
	uclick3,
	uclick5,
	uclick7,
	uclick15,
	uclick25,

      ubuy1,
      ubuy3,
      ubuy5,
      ubuy7,
      ubuy15,
      ubuy25,

      ucollect1,
      ucollect3,
	ucollect5,
	ucollect7,
	ucollect15,
	ucollect25,

	ubasket1,
	ubasket3,
	ubasket5,
	ubasket7,
	ubasket15,
	ubasket25 ,

	asked_users ,
   	n_clicked,
 	n_bought,
	n_collected,
	n_basketed,
	actions_by ,

      bclicked1,
	bclicked3,
	bclicked5,
 	bclicked7,
 	bclicked15,
 	bclicked25,

       bbought1,
       bbought3,
       bbought5,
       bbought7,
       bbought15,
       bbought25,

	 bcollected1,
	 bcollected3,
	 bcollected5,
	 bcollected7,
	 bcollected15,
 	 bcollected25,

	bbasketed1,
 	bbasketed3,
	bbasketed5,
 	bbasketed7,
	bbasketed15,
 	bbasketed25

from
 lm_user_brand_features_5_20_7_15

left outer join
lm_user_features_5_20_7_15
on 	lm_user_brand_features_5_20_7_15.user_id = lm_user_features_5_20_7_15.user_id

left outer join
lm_brand_features_5_20_7_15
on 	lm_user_brand_features_5_20_7_15.brand_id = lm_brand_features_5_20_7_15.brand_id		

left outer join
lm_user_date_features_5_20_7_15
on 	lm_user_brand_features_5_20_7_15.user_id = lm_user_date_features_5_20_7_15.user_id

left outer join
lm_brand_date_features_5_20_7_15
on 	lm_user_brand_features_5_20_7_15.brand_id = lm_brand_date_features_5_20_7_15.brand_id		

where n_actions is not null and  actions_by is not null   ; --fliter
""")


