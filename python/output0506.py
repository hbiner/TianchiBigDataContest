sql("""

create table t_tmall_add_user_brand_predict_dh as
select
    user_id,
    wm_concat(',',brand_id) as brand
from(
select distinct user_id,brand_id
from(
  select user_id,brand_id
  from(
    select
        user_id,
        brand_id,
        row_number() over (partition by user_id order by click_day desc,last_day desc,n_click desc) as rank
    from(
        select
            user_id,
            brand_id,
            n_click,
            click_day,
            last_day
        from
            yi_predict_user_brand_information
        where last_day>'08-08'and n_click>4
    )a
  )b
  where rank <= 5
     union all
  select user_id,brand_id from yi_predict_set where type=1 and visit_datetime>'08-01'
)c)d
group by user_id;

""")