sql("""
create table yi_user_information as 
select distinct t_alibaba_bigdata_user_brand_total_1.user_id,
       case
       when firstmonth_buy.buy_num is null then 0
       when firstmonth_buy.buy_num is not null then firstmonth_buy.buy_num
       end as firstbuy,
       case
       when secondmonth_buy.buy_num is null then 0
       when secondmonth_buy.buy_num is not null then secondmonth_buy.buy_num
       end as secondbuy,
       case
       when thirdmonth_buy.buy_num is null then 0
       when thirdmonth_buy.buy_num is not null then thirdmonth_buy.buy_num
       end as thirdbuy,
       case
       when fourthmonth_buy.buy_num is null then 0
       when fourthmonth_buy.buy_num is not null then fourthmonth_buy.buy_num
       end as fourthbuy,
       case
       when firstmonth_click.click_num is null then 0
       when firstmonth_click.click_num is not null then firstmonth_click.click_num
       end as firstclick,
       case
       when secondmonth_click.click_num is null then 0
       when secondmonth_click.click_num is not null then secondmonth_click.click_num
       end as secondclick,
       case
       when thirdmonth_click.click_num is null then 0
       when thirdmonth_click.click_num is not null then thirdmonth_click.click_num
       end as thirdclick,
       case
       when fourthmonth_click.click_num is null then 0
       when fourthmonth_click.click_num is not null then fourthmonth_click.click_num
       end as fourthclick,
       datetime_1.firstday as firstday,
       datetime_1.lastday as lastday
from t_alibaba_bigdata_user_brand_total_1
left outer join(
    select user_id, count(distinct brand_id) as buy_num
    from t_alibaba_bigdata_user_brand_total_1 
    where type='1' and visit_datetime<='05-15'
    group by user_id) firstmonth_buy
on t_alibaba_bigdata_user_brand_total_1.user_id=firstmonth_buy.user_id
left outer join(
    select user_id, count(distinct brand_id) as buy_num 
    from t_alibaba_bigdata_user_brand_total_1 
    where type='1' and visit_datetime<='06-15' and visit_datetime>'05-15'
    group by user_id) secondmonth_buy
on t_alibaba_bigdata_user_brand_total_1.user_id=secondmonth_buy.user_id
left outer join(
    select user_id, count(distinct brand_id) as buy_num 
    from t_alibaba_bigdata_user_brand_total_1 
    where type='1' and visit_datetime<='07-15' and visit_datetime>'06-15'
    group by user_id) thirdmonth_buy
on t_alibaba_bigdata_user_brand_total_1.user_id=thirdmonth_buy.user_id
left outer join(
    select user_id, count(distinct brand_id) as buy_num    
    from t_alibaba_bigdata_user_brand_total_1 
    where type='1' and visit_datetime<='08-15' and visit_datetime>'07-15'
    group by user_id) fourthmonth_buy 
on t_alibaba_bigdata_user_brand_total_1.user_id=fourthmonth_buy.user_id
left outer join(
    select user_id, count(brand_id) as click_num 
    from t_alibaba_bigdata_user_brand_total_1 
    where type='0' and visit_datetime<='05-15'
    group by user_id) firstmonth_click
on t_alibaba_bigdata_user_brand_total_1.user_id=firstmonth_click.user_id
left outer join(
    select user_id, count(brand_id) as click_num 
    from t_alibaba_bigdata_user_brand_total_1
    where type='0' and visit_datetime<='06-15' and visit_datetime>'05-15'
    group by user_id) secondmonth_click
on t_alibaba_bigdata_user_brand_total_1.user_id=secondmonth_click.user_id
left outer join(
    select user_id, count(brand_id) as click_num 
    from t_alibaba_bigdata_user_brand_total_1
    where type='0' and visit_datetime<='07-15' and visit_datetime>'06-15'
    group by user_id) thirdmonth_click
on t_alibaba_bigdata_user_brand_total_1.user_id=thirdmonth_click.user_id
left outer join(
    select user_id, count(brand_id) as click_num 
    from t_alibaba_bigdata_user_brand_total_1 
    where type='0' and visit_datetime<='08-15' and visit_datetime>'07-15'
    group by user_id) fourthmonth_click
on t_alibaba_bigdata_user_brand_total_1.user_id=fourthmonth_click.user_id
left outer join(
    select user_id,max(visit_datetime) as lastday,min(visit_datetime) as firstday
    from t_alibaba_bigdata_user_brand_total_1
    group by user_id) datetime_1
on t_alibaba_bigdata_user_brand_total_1.user_id=datetime_1.user_id
;
""")
