create table yi_user_brand_information as
select distinct t_alibaba_bigdata_user_brand_total_1.user_id,
                t_alibaba_bigdata_user_brand_total_1.brand_id,
                case 
                when nclick.n_click is null then 0
                when nclick.n_click is not null then nclick.n_click
                end as n_click,
                case 
                when nbuy.n_buy is null then 0
                when nbuy.n_buy is not null then nbuy.n_buy
                end as n_buy,
                case 
                when ncollect.n_collect is null then 0
                when ncollect.n_collect is not null then ncollect.n_collect
                end as n_collect,
                case 
                when nbasket.n_basket is null then 0
                when nbasket.n_basket is not null then nbasket.n_basket
                end as n_basket,
                case 
                when clickday.click_day is null then 0
                when clickday.click_day is not null then clickday.click_day
                end as click_day,
                case 
                when buyday.buy_day is null then 0
                when buyday.buy_day is not null then buyday.buy_day
                end as buy_day,
                firstday.first_day,
                lastday.last_day
from t_alibaba_bigdata_user_brand_total_1
left outer join(    
    select user_id,brand_id,count(brand_id) as n_click
    from t_alibaba_bigdata_user_brand_total_1
    where type='0'
    group by user_id,brand_id) nclick
on t_alibaba_bigdata_user_brand_total_1.user_id=nclick.user_id and t_alibaba_bigdata_user_brand_total_1.brand_id=nclick.brand_id
left outer join(    
    select user_id,brand_id,count(brand_id) as n_buy
    from t_alibaba_bigdata_user_brand_total_1
    where type='1'
    group by user_id,brand_id) nbuy
on t_alibaba_bigdata_user_brand_total_1.user_id=nbuy.user_id and t_alibaba_bigdata_user_brand_total_1.brand_id=nbuy.brand_id
left outer join(    
    select user_id,brand_id,count(brand_id) as n_collect
    from t_alibaba_bigdata_user_brand_total_1
    where type='2'
    group by user_id,brand_id) ncollect
on t_alibaba_bigdata_user_brand_total_1.user_id=ncollect.user_id and t_alibaba_bigdata_user_brand_total_1.brand_id=ncollect.brand_id
left outer join(    
    select user_id,brand_id,count(brand_id) as n_basket
    from t_alibaba_bigdata_user_brand_total_1
    where type='3'
    group by user_id,brand_id) nbasket
on t_alibaba_bigdata_user_brand_total_1.user_id=nbasket.user_id and t_alibaba_bigdata_user_brand_total_1.brand_id=nbasket.brand_id
left outer join(    
    select user_id,brand_id,count(distinct visit_datetime) as click_day
    from t_alibaba_bigdata_user_brand_total_1
    where type='0'
    group by user_id,brand_id) clickday
on t_alibaba_bigdata_user_brand_total_1.user_id=clickday.user_id and t_alibaba_bigdata_user_brand_total_1.brand_id=clickday.brand_id
left outer join(    
    select user_id,brand_id,count(distinct visit_datetime) as buy_day
    from t_alibaba_bigdata_user_brand_total_1
    where type='1'
    group by user_id,brand_id) buyday
on t_alibaba_bigdata_user_brand_total_1.user_id=buyday.user_id and t_alibaba_bigdata_user_brand_total_1.brand_id=buyday.brand_id
left outer join(    
    select user_id,brand_id,min(visit_datetime) as first_day
    from t_alibaba_bigdata_user_brand_total_1
    group by user_id,brand_id) firstday
on t_alibaba_bigdata_user_brand_total_1.user_id=firstday.user_id and t_alibaba_bigdata_user_brand_total_1.brand_id=firstday.brand_id
left outer join(    
    select user_id,brand_id,max(visit_datetime) as last_day
    from t_alibaba_bigdata_user_brand_total_1
    group by user_id,brand_id) lastday
on t_alibaba_bigdata_user_brand_total_1.user_id=lastday.user_id and t_alibaba_bigdata_user_brand_total_1.brand_id=lastday.brand_id


  


