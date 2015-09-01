
create table yi_brand_information as
select distinct t_alibaba_bigdata_user_brand_total_1.brand_id,
       touchuser.touch_user,
       case 
       when touchclick.touch_click is null then 0
       when touchclick.touch_click is not null then touchclick.touch_click
       end as touch_click,
       case 
       when touchbuy.touch_buy is null then 0
       when touchbuy.touch_buy is not null then touchbuy.touch_buy
       end as touch_buy,       
       case 
       when touchcollect.touch_collect is null then 0
       when touchcollect.touch_collect is not null then touchcollect.touch_collect
       end as touch_collect,
       case 
       when touchbasket.touch_basket is null then 0
       when touchbasket.touch_basket is not null then touchbasket.touch_basket
       end as touch_basket,
       firstday.first_day,
       lastday.last_day
from t_alibaba_bigdata_user_brand_total_1
left outer join(
    select brand_id, count(distinct user_id) as touch_user
    from t_alibaba_bigdata_user_brand_total_1 
    group by brand_id) touchuser
on t_alibaba_bigdata_user_brand_total_1.brand_id=touchuser.brand_id
left outer join(
    select brand_id, min(visit_datetime) as first_day
    from t_alibaba_bigdata_user_brand_total_1 
    group by brand_id) firstday
on t_alibaba_bigdata_user_brand_total_1.brand_id=firstday.brand_id
left outer join(
    select brand_id, max(visit_datetime) as last_day
    from t_alibaba_bigdata_user_brand_total_1 
    group by brand_id) lastday
on t_alibaba_bigdata_user_brand_total_1.brand_id=lastday.brand_id
left outer join(

    select brand_id, count(user_id) as touch_click
    from t_alibaba_bigdata_user_brand_total_1 
    where type='0'
    group by brand_id) touchclick
on t_alibaba_bigdata_user_brand_total_1.brand_id=touchclick.brand_id
left outer join(
    select brand_id, count(distinct user_id) as touch_buy
    from t_alibaba_bigdata_user_brand_total_1 
    where type='1'
    group by brand_id) touchbuy
on t_alibaba_bigdata_user_brand_total_1.brand_id=touchbuy.brand_id
left outer join(
    select brand_id, count(distinct user_id) as touch_collect
    from t_alibaba_bigdata_user_brand_total_1 
    where type='2'
    group by brand_id) touchcollect
on t_alibaba_bigdata_user_brand_total_1.brand_id=touchcollect.brand_id
left outer join(
    select brand_id, count(distinct user_id) as touch_basket
    from t_alibaba_bigdata_user_brand_total_1 
    where type='3'
    group by brand_id) touchbasket
on t_alibaba_bigdata_user_brand_total_1.brand_id=touchbasket.brand_id
;




