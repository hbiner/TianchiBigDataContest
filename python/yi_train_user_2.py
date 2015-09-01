create table yi_train_user_2 as 
select yi_train_user.user_id,
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
       when n_buy!=0 then collect_buy/n_buy
       end as collectbuy,
       case 
       when n_buy=0 then 0
       when n_buy!=0 then basket_buy/n_buy
       end as basketbuy,
       case 
       when click_day=0 then 0
       when click_day!=0 then buy_day/click_day 
       end as clickbuy_day,
       last_day/92 as lastday,
       case
       when week.num is null then 0
       when week.num is not null then week.num
       end as oneweek,
       case
       when month.num is null then 0
       when month.num is not null then month.num
       end as onemonth
from yi_train_user
left outer join
   (select distinct user_id,1 as num 
    from yi_train_set
    where type=1 and visit_datetime>'07-08')week
on week.user_id=yi_train_user.user_id
left outer join
   (select distinct user_id,1 as num 
    from yi_train_set
    where type=1 and visit_datetime>='07-01')month
on month.user_id=yi_train_user.user_id;

