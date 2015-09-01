




sql("""
drop table if exists lm_t_tmall_add_user_brand_predict_dh ; --
create table lm_t_tmall_add_user_brand_predict_dh as  --
select --  first
    user_id, 
    wm_concat(',',brand_id) as brand -- connect the result
from
   ( select
	user_id ,
   	brand_id 
  from lm_lr_prediction_output
	where probability >= 0.481
) a
  
group by user_id;  
""")
