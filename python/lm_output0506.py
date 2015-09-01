sql("""
--- drop the predict table exsited
drop table lm_t_tmall_add_user_brand_predict_dh ; 
create table lm_t_tmall_add_user_brand_predict_dh as
select --  first
    user_id, 
    wm_concat(',',brand_id) as brand -- connect the result
from(
	select  distinct user_id,brand_id --second
     from(
          select user_id,brand_id  --third
          from(
               select   -- fourth
          		  user_id,
          		  brand_id,
          		  row_number() over (partition by user_id order by click_day desc,last_day desc,n_click desc) as rank
    		     from(
        		  select  -- fifth
                	user_id,
            		brand_id,
            		n_click,
            		click_day,
            		last_day,
				n_buy
      		  from
                 yi_predict_user_brand_information
      		  where last_day>'08-08'and n_click>4 and n_buy>1 -- screening condition
   		     )a
  		  )b
    where rank <= 5

    union all
    select user_id,brand_id from yi_predict_set where type=1 and visit_datetime>'08-01'
    )c
)d

group by user_id;

""")
