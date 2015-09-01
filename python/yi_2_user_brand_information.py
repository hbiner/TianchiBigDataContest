create table yi_2_user_brand_information as
select yi_user_brand_information.user_id,
       yi_user_brand_information.brand_id,
       case 
       when max.maxnclick!=0 then yi_user_brand_information.n_click/max.maxnclick 
       when max.maxnclick=0 then yi_user_brand_information.n_click end as n_click_after,
       case 
       when max.maxnbuy!=0 then yi_user_brand_information.n_buy/max.maxnbuy
       when max.maxnbuy=0 then yi_user_brand_information.n_buy end as n_buy_after,
       case 
       when max.maxncollect!=0 then yi_user_brand_information.n_collect/max.maxncollect
       when max.maxncollect=0 then yi_user_brand_information.n_collect end as n_collect_after,
       case 
       when max.maxnbasket!=0 then yi_user_brand_information.n_basket/max.maxnbasket
       when max.maxnbasket=0 then yi_user_brand_information.n_basket end as n_basket_after,
       case 
       when max.maxclickday!=0 then yi_user_brand_information.click_day/max.maxclickday
       when max.maxclickday=0 then yi_user_brand_information.click_day end as click_day_after,
       case 
       when max.maxbuyday!=0 then yi_user_brand_information.buy_day/max.maxbuyday
       when max.maxbuyday=0 then yi_user_brand_information.buy_day end as buy_day_after,
       datediff(to_date(concat('2013-',last_day),'yyyy-mm-dd'),to_date('2013-04-14','yyyy-mm-dd'),'dd') as last_day_after
from  yi_user_brand_information
left outer join 
(select user_id,
        max(n_click) as maxnclick,
        max(n_buy) as maxnbuy,
        max(n_collect) as maxncollect,
        max(n_basket) as maxnbasket,
        max(click_day) as maxclickday,
        max(buy_day) as maxbuyday
from yi_user_brand_information
group by user_id)max
on yi_user_brand_information.user_id=max.user_id;
