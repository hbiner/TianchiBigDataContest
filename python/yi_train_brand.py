create table yi_train_brand as
select distinct yi_train_set.brand_id,
                case 
                when click.num is null then 0
                when click.num is not null then click.num
                end as n_click,
                case 
                when buy.num is null then 0
                when buy.num is not null then buy.num
                end as n_buy,
                case 
                when collect.num is null then 0
                when collect.num is not null then collect.num
                end as n_collect,
                case 
                when basket.num is null then 0
                when basket.num is not null then basket.num
                end as n_basket,
                case 
                when nbuy.num is null then 0
                when nbuy.num is not null then nbuy.num
                end as n_nbuy,
                case 
                when clickday.num is null then 0
                when clickday.num is not null then clickday.num
                end as n_clickday,
                case 
                when buyday.num is null then 0
                when buyday.num is not null then buyday.num
                end as n_buyday,
                lastday.num as last_day
from yi_train_set
LEFT OUTER JOIN     
     (select brand_id,
             count(distinct user_id) as num
      from yi_train_set
      where type=0
      group by brand_id)click
on click.brand_id=yi_train_set.brand_id
left outer join
     (select brand_id,
             count(distinct user_id) as num
      from yi_train_set
      where type=1
      group by brand_id)buy
on buy.brand_id=yi_train_set.brand_id
left outer join
     (select brand_id,
             count(distinct user_id) as num
      from yi_train_set
      where type=2
      group by brand_id)collect
on collect.brand_id=yi_train_set.brand_id
left outer join
     (select brand_id,
             count(distinct user_id) as num
      from yi_train_set
      where type=3
      group by brand_id)basket
on basket.brand_id=yi_train_set.brand_id
left outer join
     (select brand_id,
             count(user_id) as num
      from yi_train_set
      where type=1
      group by brand_id)nbuy
on nbuy.brand_id=yi_train_set.brand_id
left outer join
     (select brand_id,
             count(distinct visit_datetime) as num
      from yi_train_set
      where type=0
      group by brand_id)clickday
on clickday.brand_id=yi_train_set.brand_id
left outer join
     (select brand_id,
             count(distinct visit_datetime) as num
      from yi_train_set
      where type=1
      group by brand_id)buyday
on buyday.brand_id=yi_train_set.brand_id
left outer join 
    (select brand_id,
            datediff(
            to_date('2013-07-16','yyyy-mm-dd'),
            to_date(concat('2013-',max(visit_datetime)),'yyyy-mm-dd'),
            'dd') as num
    from yi_train_set
    group by brand_id)lastday
on lastday.brand_id=yi_train_set.brand_id;

