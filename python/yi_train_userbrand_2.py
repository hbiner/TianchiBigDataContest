create table yi_train_userbrand_2 as
select yi_train_userbrand.user_id,
       yi_train_userbrand.brand_id,
       case
       when maxclick.num=0 then 0
       when maxclick.num!=0 then n_click/maxclick.num
       end as n_click,
       case
       when maxbuy.num=0 then 0
       when maxbuy.num!=0 then n_buy/maxbuy.num
       end as n_buy,
       case
       when maxclickday.num=0 then 0
       when maxclickday.num!=0 then n_clickday/maxclickday.num
       end as n_clickday,
       case
       when maxbuyday.num=0 then 0
       when maxbuyday.num!=0 then n_buyday/maxbuyday.num
       end as nbuyday,
       case 
       when n_buyday>1 then 1
       when n_buyday<=1 then 0
       end as repeatbuy,
       n_collect,
       n_basket,
       n_lastday/92 as n_lastday,
       case
       when collectbuy.num is null then 0
       when collectbuy.num is not null then collectbuy.num
       end as collectbuy,
       case
       when basketbuy.num is null then 0
       when basketbuy.num is not null then basketbuy.num
       end as basketbuy,
       case
       when week.num is null then 0
       when week.num is not null then week.num
       end as weekbuy,
       case 
       when month.num is null then 0
       when month.num is not null then month.num
       end as monthbuy
from yi_train_userbrand
left outer join 
    (select user_id,
            max(n_click) as num
     from yi_train_userbrand
     group by user_id)maxclick
on yi_train_userbrand.user_id=maxclick.user_id
left outer join 
    (select user_id,
            max(n_buy) as num
     from yi_train_userbrand
     group by user_id)maxbuy
on yi_train_userbrand.user_id=maxbuy.user_id
left outer join 
    (select user_id,
            max(n_clickday) as num
     from yi_train_userbrand
     group by user_id)maxclickday
on yi_train_userbrand.user_id=maxclickday.user_id
left outer join 
    (select user_id,
            max(n_buyday) as num
     from yi_train_userbrand
     group by user_id)maxbuyday
on yi_train_userbrand.user_id=maxbuyday.user_id 
left outer join
    (select distinct collecttime.user_id,
            collecttime.brand_id,
            1 as num
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
     where collecttime.collect_time<=buytime.buy_time)collectbuy
on collectbuy.user_id=yi_train_userbrand.user_id
and collectbuy.brand_id=yi_train_userbrand.brand_id
left outer join
    (select distinct baskettime.user_id,
            baskettime.brand_id,
            1 as num
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
     where baskettime.basket_time<=buytime.buy_time)basketbuy
on basketbuy.user_id=yi_train_userbrand.user_id
and basketbuy.brand_id=yi_train_userbrand.brand_id   
left outer join
    (select distinct user_id,brand_id,1 as num
     from yi_train_set
     where type=1 and visit_datetime>'07-08')week
on week.user_id=yi_train_userbrand.user_id
and week.brand_id=yi_train_userbrand.brand_id
left outer join
    (select distinct user_id,brand_id,1 as num
     from yi_train_set
     where type=1 and visit_datetime>='07-01')month
on month.user_id=yi_train_userbrand.user_id
and month.brand_id=yi_train_userbrand.brand_id  
