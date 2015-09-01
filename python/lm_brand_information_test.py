sql("""
drop table if exists lm_user_brand_information_train ;
create table lm_user_brand_information_train as
select distinct small_sample.user_id,
                small_sample.brand_id,
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
                end as buy_day
from small_sample
left outer join(    
    select user_id,brand_id,count(brand_id) as n_click
    from small_sample
    where type='0' and visit_datetime < "07-16" 
    group by user_id,brand_id) nclick
on small_sample.user_id=nclick.user_id and small_sample.brand_id=nclick.brand_id
left outer join(    
    select user_id,brand_id,count(brand_id) as n_buy
    from  small_sample
    where type='1' and visit_datetime < "07-16"
    group by user_id,brand_id) nbuy
on  small_sample.user_id=nbuy.user_id and small_sample.brand_id=nbuy.brand_id
left outer join(    
    select user_id,brand_id,count(brand_id) as n_collect
    from small_sample 
    where type='2' and visit_datetime < "07-16"
    group by user_id,brand_id) ncollect
on  small_sample.user_id=ncollect.user_id and  small_sample.brand_id=ncollect.brand_id
left outer join(    
    select user_id,brand_id,count(brand_id) as n_basket
    from small_sample
    where type='3' and visit_datetime < "07-16"
    group by user_id,brand_id) nbasket
on  small_sample.user_id=nbasket.user_id and  small_sample.brand_id=nbasket.brand_id
left outer join(    
    select user_id,brand_id,count(distinct visit_datetime) as click_day
    from  small_sample
    where type='0' and visit_datetime < "07-16"
    group by user_id,brand_id) clickday
on  small_sample.user_id=clickday.user_id and  small_sample.brand_id=clickday.brand_id
left outer join(    
    select user_id,brand_id,count(distinct visit_datetime) as buy_day
    from small_sample
    where type='1' and visit_datetime < "07-16"
    group by user_id,brand_id) buyday
on   small_sample.user_id=buyday.user_id and   small_sample.brand_id=buyday.brand_id
;
""")

  


