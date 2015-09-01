create table yi_train_user as
select distinct yi_train_set.user_id,
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
       when clickday.num is null then 0
       when clickday.num is not null then clickday.num
       end as click_day,
       case 
       when buyday.num is null then 0
       when buyday.num is not null then buyday.num
       end as buy_day,
       case 
       when lastact.num is null then 0
       when lastact.num is not null then lastact.num
       end as last_day,
       case 
       when collectbuy.num is null then 0
       when collectbuy.num is not null then collectbuy.num
       end as collect_buy,
       case 
       when basketbuy.num is null then 0
       when basketbuy.num is not null then basketbuy.num
       end as basket_buy
from yi_train_set
left outer join 
    (select user_id,
            count(distinct brand_id) as num
    from yi_train_set
    where type=0
    group by user_id)click
on click.user_id=yi_train_set.user_id
left outer join 
    (select user_id,
            count(distinct brand_id) as num
    from yi_train_set
    where type=1
    group by user_id)buy
on buy.user_id=yi_train_set.user_id
left outer join 
    (select user_id,
            count(distinct brand_id) as num
    from yi_train_set
    where type=2
    group by user_id)collect
on collect.user_id=yi_train_set.user_id
left outer join 
    (select user_id,
            count(distinct brand_id) as num
    from yi_train_set
    where type=3
    group by user_id)basket
on basket.user_id=yi_train_set.user_id
left outer join 
    (select user_id,
            count(distinct visit_datetime) as num
    from yi_train_set
    where type=0
    group by user_id)clickday
on clickday.user_id=yi_train_set.user_id
left outer join 
    (select user_id,
            count(distinct visit_datetime) as num
    from yi_train_set
    where type=1
    group by user_id)buyday
on buyday.user_id=yi_train_set.user_id
left outer join 
    (select user_id,
            datediff(
            to_date('2013-07-16','yyyy-mm-dd'),
            to_date(concat('2013-',max(visit_datetime)),'yyyy-mm-dd'),
            'dd') as num
    from yi_train_set
    group by user_id)lastact
on lastact.user_id=yi_train_set.user_id
left outer join
    (select collecttime.user_id,
            count(distinct collecttime.brand_id) as num
     from
         (select distinct user_id,
                         brand_id,
                         visit_datetime as collect_time
          from yi_train_set
          where type=2)collecttime
     left outer join
          (select user_id,brand_id,max(visit_datetime) as buy_time
          from yi_train_set
          where type=1
          group by user_id,brand_id)buytime
     on collecttime.user_id=buytime.user_id and collecttime.brand_id=buytime.brand_id
     where collecttime.collect_time<=buytime.buy_time 
     group by collecttime.user_id)collectbuy
on collectbuy.user_id=yi_train_set.user_id
left outer join
    (select baskettime.user_id,
            count(distinct baskettime.brand_id) as num
     from
         (select distinct user_id,
                          brand_id,
                          visit_datetime as basket_time
          from yi_train_set
          where type=3)baskettime
     left outer join
         (select user_id,brand_id,max(visit_datetime) as buy_time
          from yi_train_set
          where type=1
          group by user_id,brand_id)buytime
     on baskettime.user_id=buytime.user_id and baskettime.brand_id=buytime.brand_id
     where baskettime.basket_time<=buytime.buy_time 
     group by baskettime.user_id)basketbuy
on basketbuy.user_id=yi_train_set.user_id
