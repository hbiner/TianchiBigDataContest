create table yi_train_userbrand as
select distinct yi_train_set.user_id,
                yi_train_set.brand_id,
                case 
                when click.num is null then 0
                when click.num is not null then click.num
                end as n_click,
                case 
                when buy.num is null then 0
                when buy.num is not null then buy.num
                end as n_buy,
                case 
                when clickday.num is null then 0
                when clickday.num is not null then clickday.num
                end as n_clickday,
                case 
                when buyday.num is null then 0
                when buyday.num is not null then buyday.num
                end as n_buyday,
                case 
                when collect.num is null then 0
                when collect.num is not null then collect.num
                end as n_collect,
                case 
                when basket.num is null then 0
                when basket.num is not null then basket.num
                end as n_basket,
                case 
                when lastday.num is null then 0
                when lastday.num is not null then lastday.num
                end as n_lastday
from yi_train_set
left outer join
    (select user_id,
            brand_id,
            count(visit_datetime) as num
     from yi_train_set
     where type=0
     group by user_id,brand_id)click 
on click.user_id=yi_train_set.user_id
and click.brand_id=yi_train_set.brand_id
left outer join
    (select user_id,
            brand_id,
            count(visit_datetime) as num
     from yi_train_set
     where type=1
     group by user_id,brand_id)buy
on buy.user_id=yi_train_set.user_id
and buy.brand_id=yi_train_set.brand_id
left outer join
    (select user_id,
            brand_id,
            count(distinct visit_datetime) as num
     from yi_train_set
     where type=0
     group by user_id,brand_id)clickday
on clickday.user_id=yi_train_set.user_id
and clickday.brand_id=yi_train_set.brand_id
left outer join
    (select user_id,
            brand_id,
            count(distinct visit_datetime) as num
     from yi_train_set
     where type=1
     group by user_id,brand_id)buyday 
on buyday.user_id=yi_train_set.user_id
and buyday.brand_id=yi_train_set.brand_id
left outer join
    (select user_id,
            brand_id,
            1 as num 
     from yi_train_set
     where type=2
     group by user_id,brand_id)collect
on collect.user_id=yi_train_set.user_id
and collect.brand_id=yi_train_set.brand_id
left outer join
    (select user_id,
            brand_id,
            1 as num 
     from yi_train_set
     where type=3
     group by user_id,brand_id)basket
on basket.user_id=yi_train_set.user_id
and basket.brand_id=yi_train_set.brand_id
left outer join
    (select user_id,
            brand_id,
            datediff(
            to_date('2013-07-16','yyyy-mm-dd'),
            to_date(concat('2013-',max(visit_datetime)),'yyyy-mm-dd'),
            'dd') as num
     from yi_train_set
     group by user_id,brand_id)lastday
on lastday.user_id=yi_train_set.user_id
and lastday.brand_id=yi_train_set.brand_id
