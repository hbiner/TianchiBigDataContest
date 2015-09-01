create table yi_train_brand_2 as
select yi_train_brand.brand_id,
       case 
       when n_click=0 then 0
       when n_click!=0 then n_buy/n_click
       end as clickbuy,
       case 
       when n_click=0 then 0
       when n_click!=0 then n_collect/n_click
       end as clickcollect,
       case 
       when n_click=0 then 0
       when n_click!=0 then n_basket/n_click
       end as clickbasket,
       case 
       when n_buy=0 then 0
       when n_buy!=0 then n_buy/n_nbuy
       end as averagebuy,
       case
       when n_clickday=0 then 0
       when n_clickday!=0 then n_buyday/n_clickday
       end as clickbuyday,
       last_day/92 as lastday,
       case
       when n_buy=0 or b.num is null then 0
       when n_buy!=0 then b.num/n_buy
       end as repeatbuy,
       case
       when week.num is null then 0
       when week.num is not null then week.num
       end as weekbuy,
       case
       when month.num is null then 0
       when month.num is not null then month.num
       end as monthbuy
from yi_train_brand
left outer join       
   (select brand_id,count(user_id) as num
    from   
         (select  brand_id,
                  user_id,
                  count(distinct visit_datetime) as num
          from yi_train_set
          where type=1
          group by brand_id,user_id)a
    where a.num>1
    group by brand_id)b
on b.brand_id=yi_train_brand.brand_id
left outer join
    (select distinct brand_id,1 as num
     from yi_train_set
     where type=1 and visit_datetime>'07-08')week
on week.brand_id=yi_train_brand.brand_id
left outer join
    (select distinct brand_id,1 as num
     from yi_train_set
     where type=1 and visit_datetime>='07-01')month
on month.brand_id=yi_train_brand.brand_id
